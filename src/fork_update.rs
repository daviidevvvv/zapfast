//! Automatic 24-hour upstream release checks and one-click fork merge/rebuild.
//!
//! Keeps personal fork modifications (such as native speech-to-text in
//! [`crate::stt`]) intact while pulling tagged upstream releases from
//! `crmne/zapfast`, compiling a release binary locally, pushing to `origin`,
//! and swapping the running executable safely on exit.

use std::collections::VecDeque;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::backend::Waker;
use crate::paths::AppDirs;
use crate::updates::{CHECK_INTERVAL, Release};

const UPSTREAM_REMOTE_NAME: &str = "upstream";
const UPSTREAM_REPO_URL: &str = "https://github.com/crmne/zapfast.git";
const MAX_LOG_LINES: usize = 200;
const RETRY_BACKOFF: Duration = Duration::from_secs(15 * 60);
const CHECK_INTERVAL_SECS: u64 = CHECK_INTERVAL.as_secs();

/// Persistent state stored under [`AppDirs::state`] so 24-hour update checks
/// survive app restarts without hammering GitHub, and updated restarts can show
/// a confirmation toast.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedState {
    #[serde(default)]
    last_checked_unix: u64,
    #[serde(default)]
    available_version: Option<String>,
    #[serde(default)]
    available_url: Option<String>,
    #[serde(default)]
    just_updated_to: Option<String>,
}

/// Current lifecycle phase of a fork auto-merge & rebuild operation.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum ForkUpdateStatus {
    #[default]
    Idle,
    Running {
        version: String,
        step: String,
        progress: Option<f32>,
    },
    Restarting {
        version: String,
    },
    Failed {
        version: String,
        error: String,
    },
}

/// Summary returned from [`ForkUpdater::poll`] for `App::sweep` to apply.
#[derive(Debug, Default)]
pub struct PollOutcome {
    pub found_update: Option<Release>,
    pub clear_update: bool,
    pub manual_up_to_date: bool,
    pub check_error: Option<String>,
    pub should_quit: bool,
}

enum WorkerEvent {
    CheckCompleted {
        manual: bool,
        checked_at_unix: u64,
        result: Result<Option<Release>, String>,
    },
    Progress {
        version: String,
        step: String,
        progress: Option<f32>,
        log_line: Option<String>,
    },
    Restarting {
        version: String,
        state_update: PersistedState,
    },
    Failed {
        version: String,
        error: String,
        log_line: Option<String>,
    },
}

/// Coordinates 24h release checks and non-blocking fork merge/rebuild tasks.
pub struct ForkUpdater {
    state_file: PathBuf,
    cache_dir: PathBuf,
    repo_dir: Option<PathBuf>,
    state: PersistedState,
    status: ForkUpdateStatus,
    checking: bool,
    last_check_error: Option<String>,
    last_auto_attempt: Option<Instant>,
    logs: VecDeque<String>,
    pending_notice: Option<String>,
    waker: Waker,
    tx: Sender<WorkerEvent>,
    rx: Receiver<WorkerEvent>,
}

impl std::fmt::Debug for ForkUpdater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForkUpdater")
            .field("repo_dir", &self.repo_dir)
            .field("status", &self.status)
            .field("checking", &self.checking)
            .field("last_checked_unix", &self.state.last_checked_unix)
            .finish_non_exhaustive()
    }
}

impl ForkUpdater {
    /// Creates a new updater controller backed by `dirs.state`.
    pub fn new(dirs: &AppDirs, waker: Waker) -> Self {
        let state_file = dirs.state.join("fork_update_state.json");
        let mut state = load_state(&state_file);
        if let Some(version) = &state.available_version
            && !fastframe_update::is_newer(version, env!("CARGO_PKG_VERSION"))
        {
            state.available_version = None;
            state.available_url = None;
            let _ = save_state(&state_file, &state);
        }
        let pending_notice = state.just_updated_to.take();
        if pending_notice.is_some() {
            let _ = save_state(&state_file, &state);
        }
        let (tx, rx) = mpsc::channel();
        Self {
            state_file,
            cache_dir: dirs.cache.clone(),
            repo_dir: find_repo_dir(),
            state,
            status: ForkUpdateStatus::Idle,
            checking: false,
            last_check_error: None,
            last_auto_attempt: None,
            logs: VecDeque::new(),
            pending_notice,
            waker,
            tx,
            rx,
        }
    }

    /// Returns any release cached during the current 24-hour check window.
    pub fn cached_release(&self) -> Option<Release> {
        let version = self.state.available_version.as_ref()?;
        let url = self.state.available_url.as_ref()?;
        fastframe_update::is_newer(version, env!("CARGO_PKG_VERSION")).then(|| Release {
            version: version.clone(),
            url: url.clone(),
        })
    }

    /// Consumes a one-shot confirmation version after an automatic restart.
    pub fn take_just_updated_notice(&mut self) -> Option<String> {
        self.pending_notice.take()
    }

    pub fn has_repo(&self) -> bool {
        self.repo_dir.is_some()
    }

    pub fn repo_dir(&self) -> Option<&Path> {
        self.repo_dir.as_deref()
    }

    pub fn status(&self) -> &ForkUpdateStatus {
        &self.status
    }

    pub fn is_checking(&self) -> bool {
        self.checking
    }

    pub fn last_check_error(&self) -> Option<&str> {
        self.last_check_error.as_deref()
    }

    pub fn last_checked_unix(&self) -> u64 {
        self.state.last_checked_unix
    }

    pub fn logs(&self) -> impl Iterator<Item = &str> {
        self.logs.iter().map(String::as_str)
    }

    pub fn is_running(&self) -> bool {
        matches!(
            self.status,
            ForkUpdateStatus::Running { .. } | ForkUpdateStatus::Restarting { .. }
        )
    }

    pub fn is_active(&self) -> bool {
        !matches!(self.status, ForkUpdateStatus::Idle)
    }

    /// Starts the automatic 24-hour check when enabled, online, and due.
    pub fn maybe_start_auto_check(&mut self, enabled: bool, is_offline: bool, now_unix: u64) {
        if cfg!(test) || !enabled || is_offline || self.checking || self.is_running() {
            return;
        }
        if self
            .last_auto_attempt
            .is_some_and(|at| at.elapsed() < RETRY_BACKOFF)
        {
            return;
        }
        if !is_check_due(self.state.last_checked_unix, now_unix) {
            return;
        }
        self.last_auto_attempt = Some(Instant::now());
        self.spawn_check(false);
    }

    /// Starts an explicit user-triggered check regardless of the 24-hour timer.
    pub fn start_manual_check(&mut self) {
        if self.checking || self.is_running() {
            return;
        }
        self.last_check_error = None;
        self.spawn_check(true);
    }

    /// Starts fetching upstream, merging cleanly, building release, pushing,
    /// and restarting into the new binary.
    pub fn start_merge_and_update(&mut self, release: Release, proxy_url: Option<String>) {
        if self.is_running() {
            return;
        }
        let Some(repo_dir) = self.repo_dir.clone() else {
            self.status = ForkUpdateStatus::Failed {
                version: release.version,
                error: "Repository locale zapfast-src non trovato su questo disco.".to_owned(),
            };
            return;
        };
        self.logs.clear();
        self.status = ForkUpdateStatus::Running {
            version: release.version.clone(),
            step: "Preparazione aggiornamento...".to_owned(),
            progress: Some(0.05),
        };
        let tx = self.tx.clone();
        let waker = self.waker.clone();
        let cache_dir = self.cache_dir.clone();
        let state_snapshot = self.state.clone();
        thread::spawn(move || {
            let version = release.version.clone();
            match run_merge_and_update(
                &repo_dir,
                &cache_dir,
                &version,
                proxy_url.as_deref(),
                state_snapshot,
                &tx,
                &waker,
            ) {
                Ok(state_update) => {
                    let _ = tx.send(WorkerEvent::Restarting {
                        version,
                        state_update,
                    });
                }
                Err(error) => {
                    let _ = tx.send(WorkerEvent::Failed {
                        version,
                        error: error.clone(),
                        log_line: Some(format!("ERRORE: {error}")),
                    });
                }
            }
            waker.wake();
        });
    }

    /// Drains background worker events without blocking the UI thread.
    pub fn poll(&mut self) -> PollOutcome {
        let mut outcome = PollOutcome::default();
        while let Ok(event) = self.rx.try_recv() {
            match event {
                WorkerEvent::CheckCompleted {
                    manual,
                    checked_at_unix,
                    result,
                } => {
                    self.checking = false;
                    match result {
                        Ok(Some(release)) => {
                            self.last_check_error = None;
                            self.state.last_checked_unix = checked_at_unix;
                            self.state.available_version = Some(release.version.clone());
                            self.state.available_url = Some(release.url.clone());
                            let _ = save_state(&self.state_file, &self.state);
                            outcome.found_update = Some(release);
                        }
                        Ok(None) => {
                            self.last_check_error = None;
                            self.state.last_checked_unix = checked_at_unix;
                            self.state.available_version = None;
                            self.state.available_url = None;
                            let _ = save_state(&self.state_file, &self.state);
                            outcome.clear_update = true;
                            if manual {
                                outcome.manual_up_to_date = true;
                            }
                        }
                        Err(err) => {
                            if manual {
                                self.last_check_error = Some(err.clone());
                                outcome.check_error = Some(err);
                            }
                        }
                    }
                }
                WorkerEvent::Progress {
                    version,
                    step,
                    progress,
                    log_line,
                } => {
                    if let Some(line) = log_line {
                        self.push_log(line);
                    }
                    self.status = ForkUpdateStatus::Running {
                        version,
                        step,
                        progress,
                    };
                }
                WorkerEvent::Restarting {
                    version,
                    state_update,
                } => {
                    self.state = state_update;
                    let _ = save_state(&self.state_file, &self.state);
                    self.push_log(format!("Riavvio ZapFast v{version} avviato."));
                    self.status = ForkUpdateStatus::Restarting { version };
                    outcome.should_quit = true;
                }
                WorkerEvent::Failed {
                    version,
                    error,
                    log_line,
                } => {
                    if let Some(line) = log_line {
                        self.push_log(line);
                    }
                    self.status = ForkUpdateStatus::Failed { version, error };
                }
            }
        }
        outcome
    }

    fn spawn_check(&mut self, manual: bool) {
        self.checking = true;
        let tx = self.tx.clone();
        let waker = self.waker.clone();
        thread::spawn(move || {
            let result = crate::updates::updater()
                .and_then(|updater| updater.check())
                .map_err(|err| err.to_string());
            let _ = tx.send(WorkerEvent::CheckCompleted {
                manual,
                checked_at_unix: u64::try_from(crate::util::now()).unwrap_or(0),
                result,
            });
            waker.wake();
        });
    }

    fn push_log(&mut self, line: String) {
        if self.logs.len() >= MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(line);
    }
}

pub fn is_check_due(last_checked_unix: u64, now_unix: u64) -> bool {
    last_checked_unix == 0 || now_unix.saturating_sub(last_checked_unix) >= CHECK_INTERVAL_SECS
}

fn is_valid_fork_repo(dir: &Path) -> bool {
    dir.join(".git").exists()
        && dir.join("Cargo.toml").is_file()
        && dir.join("src").join("stt.rs").is_file()
}

fn find_repo_dir() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(env_dir) = option_env!("ZAPFAST_REPO_DIR") {
        candidates.push(PathBuf::from(env_dir));
    }
    candidates.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    if let Some(home) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
        let home_path = PathBuf::from(home);
        candidates.push(home_path.join("Downloads").join("zapfast-src"));
        candidates.push(home_path.join("zapfast-src"));
    }
    if let Ok(exe) = std::env::current_exe() {
        let mut cursor = exe.parent();
        for _ in 0..4 {
            if let Some(dir) = cursor {
                candidates.push(dir.to_path_buf());
                cursor = dir.parent();
            }
        }
    }
    candidates
        .into_iter()
        .find(|candidate| is_valid_fork_repo(candidate))
}

fn load_state(path: &Path) -> PersistedState {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn save_state(path: &Path, state: &PersistedState) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(state).map_err(std::io::Error::other)?;
    fs::write(path, json)
}

fn emit_progress(
    tx: &Sender<WorkerEvent>,
    waker: &Waker,
    version: &str,
    step: impl Into<String>,
    progress: Option<f32>,
    log_line: Option<String>,
) {
    let _ = tx.send(WorkerEvent::Progress {
        version: version.to_owned(),
        step: step.into(),
        progress,
        log_line,
    });
    waker.wake();
}

fn configure_hidden(cmd: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    #[cfg(not(windows))]
    {
        let _ = cmd;
    }
}

fn augmented_path() -> std::ffi::OsString {
    let existing = std::env::var_os("PATH").unwrap_or_default();
    let mut dirs: Vec<PathBuf> = std::env::split_paths(&existing).collect();
    let mut candidates = Vec::new();
    if let Some(home) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
        candidates.push(PathBuf::from(home).join(".cargo").join("bin"));
    }
    #[cfg(windows)]
    {
        candidates.push(PathBuf::from(r"C:\Program Files\Git\cmd"));
        candidates.push(PathBuf::from(r"C:\Strawberry\perl\bin"));
        candidates.push(PathBuf::from(r"C:\Strawberry\c\bin"));
        for edition in ["18", "2022", "2019"] {
            for sku in ["BuildTools", "Community", "Professional", "Enterprise"] {
                candidates.push(PathBuf::from(format!(
                    r"C:\Program Files (x86)\Microsoft Visual Studio\{edition}\{sku}\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin"
                )));
                candidates.push(PathBuf::from(format!(
                    r"C:\Program Files\Microsoft Visual Studio\{edition}\{sku}\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin"
                )));
            }
        }
    }
    for dir in candidates {
        if dir.is_dir() && !dirs.iter().any(|existing_dir| existing_dir == &dir) {
            dirs.insert(0, dir);
        }
    }
    std::env::join_paths(dirs).unwrap_or(existing)
}

fn detect_toolchain_override(repo_dir: &Path, path_env: &std::ffi::OsStr) -> Option<String> {
    let mut cmd = Command::new("rustup");
    cmd.args(["toolchain", "list"])
        .current_dir(repo_dir)
        .env("PATH", path_env);
    configure_hidden(&mut cmd);
    let output = cmd.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let list = String::from_utf8_lossy(&output.stdout);
    let has_active_pinned = list
        .lines()
        .any(|line| line.contains("(active)") || line.contains("(override)"));
    if has_active_pinned {
        return None;
    }
    list.lines()
        .any(|line| line.starts_with("stable"))
        .then(|| "stable".to_owned())
}

fn run_git(repo_dir: &Path, args: &[&str], proxy_url: Option<&str>) -> Result<String, String> {
    let mut cmd = Command::new("git");
    cmd.args(args)
        .current_dir(repo_dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("PATH", augmented_path());
    if let Some(proxy) = proxy_url {
        cmd.env("HTTP_PROXY", proxy).env("HTTPS_PROXY", proxy);
    }
    configure_hidden(&mut cmd);
    let output = cmd
        .output()
        .map_err(|err| format!("Impossibile avviare git ({}): {err}", args.join(" ")))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if output.status.success() {
        Ok(if stdout.is_empty() { stderr } else { stdout })
    } else {
        let detail = if stderr.is_empty() { stdout } else { stderr };
        Err(format!("git {} fallito: {detail}", args.join(" ")))
    }
}

fn ensure_upstream_remote(repo_dir: &Path, proxy_url: Option<&str>) -> Result<(), String> {
    let remotes = run_git(repo_dir, &["remote"], proxy_url)?;
    if remotes
        .lines()
        .any(|line| line.trim() == UPSTREAM_REMOTE_NAME)
    {
        Ok(())
    } else {
        run_git(
            repo_dir,
            &["remote", "add", UPSTREAM_REMOTE_NAME, UPSTREAM_REPO_URL],
            proxy_url,
        )?;
        Ok(())
    }
}

fn resolve_target_ref(repo_dir: &Path, version: &str, proxy_url: Option<&str>) -> Option<String> {
    let tag_v = format!("refs/tags/v{version}");
    let tag_plain = format!("refs/tags/{version}");
    for candidate in [
        tag_v.as_str(),
        tag_plain.as_str(),
        "refs/remotes/upstream/main",
        "refs/remotes/upstream/master",
    ] {
        let verify = format!("{candidate}^{{commit}}");
        if run_git(repo_dir, &["rev-parse", "--verify", &verify], proxy_url).is_ok() {
            return Some(candidate.to_owned());
        }
    }
    None
}

fn parse_compiling_crate(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("Compiling ")?;
    rest.split_whitespace().next()
}

fn run_cargo_build(
    repo_dir: &Path,
    version: &str,
    target_dir_override: Option<&Path>,
    proxy_url: Option<&str>,
    tx: &Sender<WorkerEvent>,
    waker: &Waker,
) -> Result<(), String> {
    let path_env = augmented_path();
    let mut cmd = Command::new("cargo");
    cmd.args(["build", "--locked", "--release"])
        .current_dir(repo_dir)
        .env("PATH", &path_env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(target_dir) = target_dir_override {
        cmd.arg("--target-dir").arg(target_dir);
    }
    if let Some(toolchain) = detect_toolchain_override(repo_dir, &path_env) {
        cmd.env("RUSTUP_TOOLCHAIN", toolchain);
    }
    if let Some(proxy) = proxy_url {
        cmd.env("HTTP_PROXY", proxy).env("HTTPS_PROXY", proxy);
    }
    configure_hidden(&mut cmd);

    let mut child = cmd
        .spawn()
        .map_err(|err| format!("Impossibile avviare cargo build: {err}"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Impossibile leggere lo stderr di cargo".to_owned())?;
    let mut reader = BufReader::new(stderr);
    let mut line = String::new();
    let mut compiled_count = 0usize;
    let mut tail_errors = VecDeque::with_capacity(20);

    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|err| format!("Errore lettura output cargo: {err}"))?;
        if read == 0 {
            break;
        }
        let clean = line.trim_end().to_owned();
        if clean.is_empty() {
            continue;
        }
        if tail_errors.len() >= 20 {
            tail_errors.pop_front();
        }
        tail_errors.push_back(clean.clone());

        if let Some(krate) = parse_compiling_crate(&clean) {
            compiled_count += 1;
            let progress = (0.35 + (compiled_count as f32 / 350.0).min(0.50)).min(0.85);
            emit_progress(
                tx,
                waker,
                version,
                format!("Compilazione release ({krate})..."),
                Some(progress),
                Some(clean),
            );
        } else if clean.contains("Finished") || clean.starts_with("error") {
            emit_progress(
                tx,
                waker,
                version,
                "Finalizzazione compilazione release...",
                Some(0.86),
                Some(clean),
            );
        }
    }

    let status = child
        .wait()
        .map_err(|err| format!("Errore attesa cargo build: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        let excerpt: Vec<_> = tail_errors.into_iter().collect();
        Err(format!(
            "Compilazione fallita ({}):\n{}",
            status,
            excerpt.join("\n")
        ))
    }
}

fn verify_built_binary(exe_path: &Path) -> Result<String, String> {
    if !exe_path.is_file() {
        return Err(format!(
            "Eseguibile compilato non trovato in {}",
            exe_path.display()
        ));
    }
    let mut cmd = Command::new(exe_path);
    cmd.arg("--version");
    configure_hidden(&mut cmd);
    let output = cmd
        .output()
        .map_err(|err| format!("Verifica --version fallita: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "Il nuovo eseguibile ha restituito codice {}",
            output.status
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn stage_and_spawn_restart(built_exe: &Path, cache_dir: &Path) -> Result<(), String> {
    let current_exe = std::env::current_exe()
        .map_err(|err| format!("Impossibile determinare l'eseguibile corrente: {err}"))?;
    let mut new_name = current_exe
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| std::ffi::OsString::from("zapfast"));
    new_name.push(".new");
    let staged_new = current_exe.with_file_name(new_name);
    if staged_new.exists() {
        let _ = fs::remove_file(&staged_new);
    }
    fs::copy(built_exe, &staged_new).map_err(|err| {
        format!(
            "Impossibile preparare il nuovo eseguibile in {}: {err}",
            staged_new.display()
        )
    })?;

    #[cfg(windows)]
    {
        fs::create_dir_all(cache_dir)
            .map_err(|err| format!("Impossibile creare la directory cache: {err}"))?;
        let script_path = cache_dir.join("zapfast-apply-update.ps1");
        let mut bak_name = current_exe
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or_else(|| std::ffi::OsString::from("zapfast"));
        bak_name.push(".bak");
        let backup_exe = current_exe.with_file_name(bak_name);
        let pid = std::process::id();
        let script = r#"param(
    [Parameter(Mandatory=$true)][int]$TargetPid,
    [Parameter(Mandatory=$true)][string]$TargetExe,
    [Parameter(Mandatory=$true)][string]$SourceExe,
    [Parameter(Mandatory=$true)][string]$BackupExe
)
$deadline = (Get-Date).AddSeconds(30)
while ((Get-Date) -lt $deadline) {
    if (-not (Get-Process -Id $TargetPid -ErrorAction SilentlyContinue)) { break }
    Start-Sleep -Milliseconds 150
}
Start-Sleep -Milliseconds 250
if (Test-Path -LiteralPath $TargetExe) {
    Copy-Item -LiteralPath $TargetExe -Destination $BackupExe -Force -ErrorAction SilentlyContinue
}
$replaced = $false
for ($i = 0; $i -lt 15; $i++) {
    try {
        Move-Item -LiteralPath $SourceExe -Destination $TargetExe -Force
        $replaced = $true
        break
    } catch {
        Start-Sleep -Milliseconds 300
    }
}
if (-not $replaced -and (Test-Path -LiteralPath $BackupExe)) {
    Copy-Item -LiteralPath $BackupExe -Destination $TargetExe -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $TargetExe) {
    Start-Process -FilePath $TargetExe | Out-Null
}
Remove-Item -LiteralPath $PSCommandPath -Force -ErrorAction SilentlyContinue
"#;
        fs::write(&script_path, script)
            .map_err(|err| format!("Impossibile scrivere lo script di sostituzione: {err}"))?;

        let mut ps = Command::new("powershell.exe");
        ps.args([
            "-NoProfile",
            "-WindowStyle",
            "Hidden",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(&script_path)
        .args(["-TargetPid", &pid.to_string(), "-TargetExe"])
        .arg(&current_exe)
        .arg("-SourceExe")
        .arg(&staged_new)
        .arg("-BackupExe")
        .arg(&backup_exe);
        configure_hidden(&mut ps);
        ps.spawn()
            .map_err(|err| format!("Impossibile avviare il helper di aggiornamento: {err}"))?;
    }

    #[cfg(not(windows))]
    {
        let _ = cache_dir;
        fs::rename(&staged_new, &current_exe)
            .map_err(|err| format!("Impossibile sostituire l'eseguibile: {err}"))?;
        Command::new(&current_exe)
            .spawn()
            .map_err(|err| format!("Impossibile riavviare ZapFast: {err}"))?;
    }

    Ok(())
}

fn run_merge_and_update(
    repo_dir: &Path,
    cache_dir: &Path,
    version: &str,
    proxy_url: Option<&str>,
    mut state: PersistedState,
    tx: &Sender<WorkerEvent>,
    waker: &Waker,
) -> Result<PersistedState, String> {
    emit_progress(
        tx,
        waker,
        version,
        "Controllo stato git locale...",
        Some(0.08),
        Some(format!("Repository locale: {}", repo_dir.display())),
    );

    let dirty = run_git(
        repo_dir,
        &["status", "--porcelain", "--untracked-files=no"],
        proxy_url,
    )?;
    if !dirty.is_empty() {
        return Err(
            "Il repository locale contiene modifiche non salvate nei file tracciati. \
             Committa o fai stash prima dell'aggiornamento automatico."
                .to_owned(),
        );
    }

    let branch = run_git(repo_dir, &["symbolic-ref", "--short", "HEAD"], proxy_url)
        .map_err(|_| "Il repository è in detached HEAD: posizionati su main.".to_owned())?;
    let pre_merge_head = run_git(repo_dir, &["rev-parse", "HEAD"], proxy_url)?;

    emit_progress(
        tx,
        waker,
        version,
        format!("Download release v{version} da crmne/zapfast..."),
        Some(0.16),
        Some("git fetch upstream --tags --prune".to_owned()),
    );
    ensure_upstream_remote(repo_dir, proxy_url)?;
    run_git(
        repo_dir,
        &["fetch", "upstream", "--tags", "--prune"],
        proxy_url,
    )?;

    let target_ref = resolve_target_ref(repo_dir, version, proxy_url).ok_or_else(|| {
        format!("Nessun tag v{version} o branch upstream/main trovato dopo il fetch.")
    })?;
    let target_sha = run_git(
        repo_dir,
        &["rev-parse", "--verify", &format!("{target_ref}^{{commit}}")],
        proxy_url,
    )?;

    let already_merged = run_git(
        repo_dir,
        &["merge-base", "--is-ancestor", &target_sha, "HEAD"],
        proxy_url,
    )
    .is_ok();

    let mut created_merge_commit = false;
    if already_merged {
        emit_progress(
            tx,
            waker,
            version,
            "Upstream già integrato in locale: avvio compilazione...",
            Some(0.28),
            Some(format!("{target_ref} già antenato di {pre_merge_head}")),
        );
    } else {
        emit_progress(
            tx,
            waker,
            version,
            format!("Merge automatico {target_ref} (preservando STT)..."),
            Some(0.25),
            Some(format!("git merge --no-edit {target_ref}")),
        );
        let merge_msg = format!("Merge upstream ZapFast v{version} (preserve STT)");
        if let Err(merge_err) = run_git(
            repo_dir,
            &["merge", "--no-edit", "-m", &merge_msg, &target_ref],
            proxy_url,
        ) {
            let conflicts = run_git(
                repo_dir,
                &["diff", "--name-only", "--diff-filter=U"],
                proxy_url,
            )
            .unwrap_or_default();
            let _ = run_git(repo_dir, &["merge", "--abort"], proxy_url);
            return Err(if conflicts.is_empty() {
                format!("Merge annullato automaticamente: {merge_err}")
            } else {
                format!(
                    "Conflitto di merge rilevato; repository ripristinato in sicurezza.\nFile in conflitto:\n{conflicts}"
                )
            });
        }
        let post_merge_head = run_git(repo_dir, &["rev-parse", "HEAD"], proxy_url)?;
        created_merge_commit = post_merge_head != pre_merge_head;
    }

    if !repo_dir.join("src").join("stt.rs").is_file() {
        if created_merge_commit {
            let _ = run_git(repo_dir, &["reset", "--hard", &pre_merge_head], proxy_url);
        }
        return Err("Verifica di sicurezza fallita: src/stt.rs mancante dopo il merge.".to_owned());
    }

    let running_from_release_dir = std::env::current_exe()
        .ok()
        .is_some_and(|exe| exe.starts_with(repo_dir.join("target").join("release")));
    let alt_target_dir = running_from_release_dir.then(|| repo_dir.join("target").join("updater"));

    emit_progress(
        tx,
        waker,
        version,
        "Compilazione release con trascrizione STT...",
        Some(0.32),
        Some("cargo build --locked --release".to_owned()),
    );

    if let Err(build_err) = run_cargo_build(
        repo_dir,
        version,
        alt_target_dir.as_deref(),
        proxy_url,
        tx,
        waker,
    ) {
        if created_merge_commit {
            let _ = run_git(repo_dir, &["reset", "--hard", &pre_merge_head], proxy_url);
        }
        return Err(build_err);
    }

    let exe_name = if cfg!(windows) {
        "zapfast.exe"
    } else {
        "zapfast"
    };
    let built_exe = alt_target_dir
        .unwrap_or_else(|| repo_dir.join("target"))
        .join("release")
        .join(exe_name);
    let version_banner = verify_built_binary(&built_exe)?;
    emit_progress(
        tx,
        waker,
        version,
        "Nuova build verificata; sincronizzazione fork GitHub...",
        Some(0.90),
        Some(format!("Build OK: {version_banner}")),
    );

    if created_merge_commit {
        match run_git(repo_dir, &["push", "origin", &branch], proxy_url) {
            Ok(_) => emit_progress(
                tx,
                waker,
                version,
                "Push su fork personale completato...",
                Some(0.94),
                Some(format!("git push origin {branch} completato")),
            ),
            Err(push_err) => emit_progress(
                tx,
                waker,
                version,
                "Installazione locale (push rimandato)...",
                Some(0.94),
                Some(format!("Avviso push origin: {push_err}")),
            ),
        }
    }

    emit_progress(
        tx,
        waker,
        version,
        "Sostituzione eseguibile e riavvio...",
        Some(0.97),
        Some(format!("Staging {}", built_exe.display())),
    );
    stage_and_spawn_restart(&built_exe, cache_dir)?;

    state.available_version = None;
    state.available_url = None;
    state.just_updated_to = Some(version.to_owned());
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_cadence_respects_24h_window() {
        assert!(is_check_due(0, 1_700_000_000));
        let last = 1_700_000_000;
        assert!(!is_check_due(last, last));
        assert!(!is_check_due(last, last + CHECK_INTERVAL_SECS - 1));
        assert!(is_check_due(last, last + CHECK_INTERVAL_SECS));
        assert!(is_check_due(last, last + CHECK_INTERVAL_SECS + 3600));
    }

    #[test]
    fn persisted_state_roundtrip_and_clears_when_current_version_matches() {
        let dir = tempfile::tempdir().unwrap();
        let dirs = AppDirs::under(dir.path());
        let state_file = dirs.state.join("fork_update_state.json");
        let saved = PersistedState {
            last_checked_unix: 1_700_000_123,
            available_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            available_url: Some("https://github.com/crmne/zapfast/releases/latest".to_owned()),
            just_updated_to: Some(env!("CARGO_PKG_VERSION").to_owned()),
        };
        save_state(&state_file, &saved).unwrap();

        let mut updater = ForkUpdater::new(&dirs, Waker::default());
        assert_eq!(updater.last_checked_unix(), 1_700_000_123);
        assert!(updater.cached_release().is_none());
        assert_eq!(
            updater.take_just_updated_notice().as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert!(updater.take_just_updated_notice().is_none());
    }

    #[test]
    fn persisted_state_exposes_newer_cached_release() {
        let dir = tempfile::tempdir().unwrap();
        let dirs = AppDirs::under(dir.path());
        let state_file = dirs.state.join("fork_update_state.json");
        let saved = PersistedState {
            last_checked_unix: 1_700_000_500,
            available_version: Some("99.0.0".to_owned()),
            available_url: Some("https://github.com/crmne/zapfast/releases/tag/v99.0.0".to_owned()),
            just_updated_to: None,
        };
        save_state(&state_file, &saved).unwrap();

        let updater = ForkUpdater::new(&dirs, Waker::default());
        assert_eq!(
            updater.cached_release(),
            Some(Release {
                version: "99.0.0".to_owned(),
                url: "https://github.com/crmne/zapfast/releases/tag/v99.0.0".to_owned(),
            })
        );
    }

    #[test]
    fn parse_compiling_crate_extracts_crate_name() {
        assert_eq!(
            parse_compiling_crate("   Compiling zapfast v0.21.2 (C:\\src)"),
            Some("zapfast")
        );
        assert_eq!(parse_compiling_crate("warning: unused var"), None);
    }

    #[test]
    fn valid_fork_repo_requires_git_manifest_and_stt() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_valid_fork_repo(dir.path()));
        fs::create_dir(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join("Cargo.toml"), "[package]").unwrap();
        assert!(!is_valid_fork_repo(dir.path()));
        fs::create_dir(dir.path().join("src")).unwrap();
        fs::write(dir.path().join("src").join("stt.rs"), "// stt").unwrap();
        assert!(is_valid_fork_repo(dir.path()));
    }
}
