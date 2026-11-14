//! Native speech-to-text transcription for WhatsApp voice notes (Groq + Google Gemini).

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use egui::{Align, CornerRadius, Id, Layout, Margin, RichText, Stroke, Ui};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

use crate::backend::Waker;
use crate::model::{Action, ChatId, Content, Message};
use crate::settings::{Settings, hex};
use crate::theme::{self, Icon, Palette};
use crate::ui::widgets;

const GROQ_URL: &str = "https://api.groq.com/openai/v1/audio/transcriptions";
const GROQ_MODELS_URL: &str = "https://api.groq.com/openai/v1/models";
const GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta/models";

pub const DEFAULT_GROQ_MODEL: &str = "whisper-large-v3-turbo";
pub const DEFAULT_LANGUAGE: &str = "it";
pub const GOOGLE_MODELS: &[&str] = &[
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
];

const MAX_GROQ_UPLOAD_BYTES: usize = 25 * 1024 * 1024;
const MAX_GOOGLE_INLINE_BYTES: usize = 14 * 1024 * 1024;
const API_CONCURRENCY: usize = 2;
const CACHE_MAX_ENTRIES: usize = 750;
const CACHE_MAX_AGE_SECS: u64 = 90 * 24 * 60 * 60;
const GROQ_MAX_ATTEMPTS: usize = 4;
const GEMINI_ATTEMPTS_PER_MODEL: usize = 2;
const HTTP_TIMEOUT_SECS: u64 = 90;
const KEY_TEST_TIMEOUT_SECS: u64 = 15;
const COPY_FEEDBACK_SECS: f32 = 1.6;
const TRANSCRIPT_MAX_WIDTH: f32 = 420.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SttProvider {
    Auto,
    Groq,
    Google,
}

impl SttProvider {
    pub const ALL: [Self; 3] = [Self::Auto, Self::Groq, Self::Google];

    pub fn from_str_lossy(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "groq" => Self::Groq,
            "google" | "gemini" => Self::Google,
            _ => Self::Auto,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Groq => "groq",
            Self::Google => "google",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "Auto (Groq -> Google)",
            Self::Groq => "Solo Groq (Whisper)",
            Self::Google => "Solo Google Gemini",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyKind {
    Groq,
    Google,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyTestState {
    Idle,
    Testing,
    Ok(String),
    Err(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedTranscript {
    pub text: String,
    pub provider: String,
    pub model: String,
    pub language: String,
    pub updated_at_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TranscriptState {
    Idle,
    WaitingDownload,
    Pending,
    Done(CachedTranscript),
    Error(String),
}

#[derive(Clone, Debug, Default)]
pub struct BulkState {
    pub active: bool,
    pub chat_id: String,
    pub generation: u64,
    pub total: usize,
    pub done: usize,
    pub failed: usize,
}

#[derive(Clone, Debug)]
pub struct SttConfig {
    pub groq_api_key: String,
    pub google_api_key: String,
    pub provider: SttProvider,
    pub language: String,
    pub model: String,
    pub debug: bool,
    pub proxy: Option<reqwest::Proxy>,
}

impl SttConfig {
    pub fn from_settings(settings: &Settings) -> Self {
        let model = settings.stt_model.trim();
        let language = settings.stt_language.trim();
        Self {
            groq_api_key: settings.stt_groq_api_key.trim().to_owned(),
            google_api_key: settings.stt_google_api_key.trim().to_owned(),
            provider: SttProvider::from_str_lossy(&settings.stt_provider),
            language: if language.is_empty() {
                DEFAULT_LANGUAGE.to_owned()
            } else {
                language.to_owned()
            },
            model: if model.is_empty() {
                DEFAULT_GROQ_MODEL.to_owned()
            } else {
                model.to_owned()
            },
            debug: settings.stt_debug,
            proxy: crate::proxy::reqwest_proxy(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PendingKeyEdit {
    pub groq: Option<String>,
    pub google: Option<String>,
    pub reveal_groq: bool,
    pub reveal_google: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct MsgKey {
    pub chat_id: String,
    pub message_id: String,
}

impl MsgKey {
    pub fn new(chat_id: &str, message_id: &str) -> Self {
        Self {
            chat_id: chat_id.to_owned(),
            message_id: message_id.to_owned(),
        }
    }

    fn cache_key(&self) -> String {
        format!("{}\n{}", self.chat_id, self.message_id)
    }

    fn from_cache_key(raw: &str) -> Option<Self> {
        let (chat_id, message_id) = raw.split_once('\n')?;
        Some(Self {
            chat_id: chat_id.to_owned(),
            message_id: message_id.to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
struct QueuedJob {
    key: MsgKey,
    path: PathBuf,
    mime: String,
    config: SttConfig,
    generation: Option<u64>,
}

enum SttEvent {
    Transcribed {
        key: MsgKey,
        generation: Option<u64>,
        result: Result<CachedTranscript, String>,
    },
    KeyTested {
        kind: KeyKind,
        result: Result<String, String>,
    },
}

struct PoolState {
    queue: VecDeque<QueuedJob>,
    cancelled_generations: HashSet<u64>,
    shutdown: bool,
}

struct WorkerPool {
    shared: Arc<(Mutex<PoolState>, Condvar)>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl WorkerPool {
    fn new(tx: Sender<SttEvent>, waker: Waker) -> Self {
        let shared = Arc::new((
            Mutex::new(PoolState {
                queue: VecDeque::new(),
                cancelled_generations: HashSet::new(),
                shutdown: false,
            }),
            Condvar::new(),
        ));
        let mut handles = Vec::with_capacity(API_CONCURRENCY);
        for index in 0..API_CONCURRENCY {
            let shared_worker = Arc::clone(&shared);
            let tx_worker = tx.clone();
            let waker_worker = waker.clone();
            let builder = thread::Builder::new().name(format!("stt-worker-{index}"));
            if let Ok(handle) = builder.spawn(move || {
                worker_loop(&shared_worker, &tx_worker, &waker_worker);
            }) {
                handles.push(handle);
            }
        }
        Self { shared, handles }
    }

    fn push(&self, job: QueuedJob) {
        let (lock, cvar) = &*self.shared;
        if let Ok(mut guard) = lock.lock() {
            if let Some(gen_id) = job.generation {
                guard.cancelled_generations.remove(&gen_id);
            }
            guard.queue.push_back(job);
            cvar.notify_one();
        }
    }

    fn cancel_generation(&self, generation: u64) -> Vec<MsgKey> {
        let (lock, _) = &*self.shared;
        let mut cancelled = Vec::new();
        if let Ok(mut guard) = lock.lock() {
            guard.cancelled_generations.insert(generation);
            let mut kept = VecDeque::with_capacity(guard.queue.len());
            while let Some(job) = guard.queue.pop_front() {
                if job.generation == Some(generation) {
                    cancelled.push(job.key);
                } else {
                    kept.push_back(job);
                }
            }
            guard.queue = kept;
        }
        cancelled
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.shared;
        if let Ok(mut guard) = lock.lock() {
            guard.shutdown = true;
            cvar.notify_all();
        }
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn worker_loop(shared: &Arc<(Mutex<PoolState>, Condvar)>, tx: &Sender<SttEvent>, waker: &Waker) {
    let (lock, cvar) = &**shared;
    loop {
        let job = {
            let mut guard = match lock.lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            while guard.queue.is_empty() && !guard.shutdown {
                guard = match cvar.wait(guard) {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
            }
            if guard.shutdown && guard.queue.is_empty() {
                return;
            }
            let Some(job) = guard.queue.pop_front() else {
                continue;
            };
            if let Some(gen_id) = job.generation
                && guard.cancelled_generations.contains(&gen_id)
            {
                let _ = tx.send(SttEvent::Transcribed {
                    key: job.key,
                    generation: job.generation,
                    result: Err("Trascrizione annullata.".to_owned()),
                });
                waker.wake();
                continue;
            }
            job
        };

        let result = run_transcription(&job.path, &job.mime, &job.config);
        let _ = tx.send(SttEvent::Transcribed {
            key: job.key,
            generation: job.generation,
            result,
        });
        waker.wake();
    }
}

#[derive(Serialize, Deserialize, Default)]
struct DiskCacheFile {
    version: u32,
    entries: HashMap<String, CachedTranscript>,
}

pub struct SttEngine {
    cache_path: PathBuf,
    states: HashMap<MsgKey, TranscriptState>,
    waiting_download: HashMap<MsgKey, (SttConfig, Option<u64>)>,
    bulk: BulkState,
    next_generation: u64,
    last_copied: Option<(MsgKey, Instant)>,
    pub groq_test: KeyTestState,
    pub google_test: KeyTestState,
    pub settings_draft: PendingKeyEdit,
    tx: Sender<SttEvent>,
    rx: Receiver<SttEvent>,
    waker: Waker,
    pool: WorkerPool,
}

impl SttEngine {
    pub fn new(cache_path: PathBuf, waker: Waker) -> Self {
        let (tx, rx) = mpsc::channel();
        let pool = WorkerPool::new(tx.clone(), waker.clone());
        let mut engine = Self {
            cache_path,
            states: HashMap::new(),
            waiting_download: HashMap::new(),
            bulk: BulkState::default(),
            next_generation: 1,
            last_copied: None,
            groq_test: KeyTestState::Idle,
            google_test: KeyTestState::Idle,
            settings_draft: PendingKeyEdit::default(),
            tx,
            rx,
            waker,
            pool,
        };
        engine.load_cache();
        engine
    }

    pub fn state(&self, chat_id: &str, message_id: &str) -> &TranscriptState {
        let key = MsgKey::new(chat_id, message_id);
        self.states.get(&key).unwrap_or(&TranscriptState::Idle)
    }

    pub fn cached_text(&self, chat_id: &str, message_id: &str) -> Option<&str> {
        match self.state(chat_id, message_id) {
            TranscriptState::Done(entry) => Some(&entry.text),
            _ => None,
        }
    }

    pub fn bulk(&self) -> &BulkState {
        &self.bulk
    }

    pub fn cache_count(&self) -> usize {
        self.states
            .values()
            .filter(|state| matches!(state, TranscriptState::Done(_)))
            .count()
    }

    pub fn was_just_copied(&self, chat_id: &str, message_id: &str, ctx: &egui::Context) -> bool {
        let Some((key, at)) = &self.last_copied else {
            return false;
        };
        if key.chat_id != chat_id || key.message_id != message_id {
            return false;
        }
        let elapsed = at.elapsed().as_secs_f32();
        if elapsed < COPY_FEEDBACK_SECS {
            let remaining = Duration::from_secs_f32((COPY_FEEDBACK_SECS - elapsed).max(0.05));
            ctx.request_repaint_after(remaining);
            true
        } else {
            false
        }
    }

    pub fn mark_copied(&mut self, chat_id: &str, message_id: &str) {
        self.last_copied = Some((MsgKey::new(chat_id, message_id), Instant::now()));
    }

    pub fn start_single(
        &mut self,
        chat_id: &str,
        message_id: &str,
        path: Option<&Path>,
        mime: &str,
        config: SttConfig,
        force: bool,
    ) -> bool {
        let key = MsgKey::new(chat_id, message_id);
        if !force {
            match self.states.get(&key) {
                Some(TranscriptState::Done(_))
                | Some(TranscriptState::Pending)
                | Some(TranscriptState::WaitingDownload) => return false,
                _ => {}
            }
        }
        if let Some(local_path) = path.filter(|candidate| candidate.is_file()) {
            self.states.insert(key.clone(), TranscriptState::Pending);
            self.pool.push(QueuedJob {
                key,
                path: local_path.to_path_buf(),
                mime: mime.to_owned(),
                config,
                generation: None,
            });
            false
        } else {
            self.states
                .insert(key.clone(), TranscriptState::WaitingDownload);
            self.waiting_download.insert(key, (config, None));
            true
        }
    }

    pub fn start_bulk(
        &mut self,
        chat_id: &str,
        items: Vec<(String, Option<PathBuf>, String)>,
        config: SttConfig,
    ) -> Vec<String> {
        self.stop_bulk();
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);

        let mut to_enqueue = Vec::new();
        let mut need_download = Vec::new();
        for (message_id, path, mime) in items {
            let key = MsgKey::new(chat_id, &message_id);
            match self.states.get(&key) {
                Some(TranscriptState::Done(_))
                | Some(TranscriptState::Pending)
                | Some(TranscriptState::WaitingDownload) => continue,
                _ => {}
            }
            if let Some(local_path) = path.filter(|candidate| candidate.is_file()) {
                self.states.insert(key.clone(), TranscriptState::Pending);
                to_enqueue.push(QueuedJob {
                    key,
                    path: local_path,
                    mime,
                    config: config.clone(),
                    generation: Some(generation),
                });
            } else {
                self.states
                    .insert(key.clone(), TranscriptState::WaitingDownload);
                self.waiting_download
                    .insert(key, (config.clone(), Some(generation)));
                need_download.push(message_id);
            }
        }

        let total = to_enqueue.len() + need_download.len();
        if total == 0 {
            return Vec::new();
        }
        self.bulk = BulkState {
            active: true,
            chat_id: chat_id.to_owned(),
            generation,
            total,
            done: 0,
            failed: 0,
        };
        for job in to_enqueue {
            self.pool.push(job);
        }
        need_download
    }

    pub fn stop_bulk(&mut self) {
        if !self.bulk.active {
            return;
        }
        let gen_id = self.bulk.generation;
        self.bulk.active = false;
        let cancelled = self.pool.cancel_generation(gen_id);
        for key in cancelled {
            if matches!(
                self.states.get(&key),
                Some(TranscriptState::Pending | TranscriptState::WaitingDownload)
            ) {
                self.states.insert(key, TranscriptState::Idle);
            }
        }
        let mut waiting_cancelled = Vec::new();
        self.waiting_download.retain(|key, (_, maybe_gen)| {
            if *maybe_gen == Some(gen_id) {
                waiting_cancelled.push(key.clone());
                false
            } else {
                true
            }
        });
        for key in waiting_cancelled {
            if matches!(
                self.states.get(&key),
                Some(TranscriptState::Pending | TranscriptState::WaitingDownload)
            ) {
                self.states.insert(key, TranscriptState::Idle);
            }
        }
    }

    pub fn has_waiting_download(&self, chat_id: &str, message_id: &str) -> bool {
        self.waiting_download
            .contains_key(&MsgKey::new(chat_id, message_id))
    }

    pub fn finish_waiting_download(
        &mut self,
        chat_id: &str,
        message_id: &str,
        mime: &str,
        result: Result<&PathBuf, String>,
    ) {
        let key = MsgKey::new(chat_id, message_id);
        let Some((config, generation)) = self.waiting_download.remove(&key) else {
            return;
        };
        if let Some(gen_id) = generation
            && (!self.bulk.active || self.bulk.generation != gen_id)
        {
            self.states.insert(key, TranscriptState::Idle);
            return;
        }
        match result {
            Ok(path) => {
                self.states.insert(key.clone(), TranscriptState::Pending);
                self.pool.push(QueuedJob {
                    key,
                    path: path.clone(),
                    mime: mime.to_owned(),
                    config,
                    generation,
                });
            }
            Err(error) => {
                self.states.insert(key, TranscriptState::Error(error));
                self.advance_bulk(generation, false);
            }
        }
    }

    pub fn test_key(&mut self, kind: KeyKind, key: String, config: SttConfig) {
        let trimmed = key.trim().to_owned();
        let state_slot = match kind {
            KeyKind::Groq => &mut self.groq_test,
            KeyKind::Google => &mut self.google_test,
        };
        if trimmed.is_empty() {
            *state_slot = KeyTestState::Err("Inserisci prima una chiave API.".to_owned());
            return;
        }
        *state_slot = KeyTestState::Testing;
        let tx = self.tx.clone();
        let waker = self.waker.clone();
        let builder = thread::Builder::new().name("stt-key-test".to_owned());
        let _ = builder.spawn(move || {
            let result = match kind {
                KeyKind::Groq => verify_groq_key(&trimmed, &config.model, config.proxy.as_ref()),
                KeyKind::Google => verify_google_key(&trimmed, config.proxy.as_ref()),
            };
            let _ = tx.send(SttEvent::KeyTested { kind, result });
            waker.wake();
        });
    }

    pub fn poll(&mut self) -> bool {
        let mut changed = false;
        let mut cache_dirty = false;
        while let Ok(event) = self.rx.try_recv() {
            changed = true;
            match event {
                SttEvent::Transcribed {
                    key,
                    generation,
                    result,
                } => match result {
                    Ok(transcript) => {
                        self.states.insert(key, TranscriptState::Done(transcript));
                        cache_dirty = true;
                        self.advance_bulk(generation, true);
                    }
                    Err(error) => {
                        self.states.insert(key, TranscriptState::Error(error));
                        self.advance_bulk(generation, false);
                    }
                },
                SttEvent::KeyTested { kind, result } => {
                    let state = match result {
                        Ok(msg) => KeyTestState::Ok(msg),
                        Err(err) => KeyTestState::Err(err),
                    };
                    match kind {
                        KeyKind::Groq => self.groq_test = state,
                        KeyKind::Google => self.google_test = state,
                    }
                }
            }
        }
        if cache_dirty {
            self.prune_and_save_cache();
        }
        changed
    }

    pub fn clear_cache(&mut self) {
        let mut removed_any = false;
        for state in self.states.values_mut() {
            if matches!(state, TranscriptState::Done(_)) {
                *state = TranscriptState::Idle;
                removed_any = true;
            }
        }
        if self.cache_path.exists() {
            let _ = fs::remove_file(&self.cache_path);
        }
        if removed_any {
            log::info!("stt cache cleared");
        }
    }

    fn advance_bulk(&mut self, generation: Option<u64>, ok: bool) {
        let Some(gen_id) = generation else {
            return;
        };
        if !self.bulk.active || self.bulk.generation != gen_id {
            return;
        }
        self.bulk.done = self.bulk.done.saturating_add(1);
        if !ok {
            self.bulk.failed = self.bulk.failed.saturating_add(1);
        }
        if self.bulk.done >= self.bulk.total {
            self.bulk.active = false;
        }
    }

    fn load_cache(&mut self) {
        let Ok(bytes) = fs::read(&self.cache_path) else {
            return;
        };
        let Ok(disk) = serde_json::from_slice::<DiskCacheFile>(&bytes) else {
            log::warn!("stt cache unreadable; ignoring");
            return;
        };
        let now = unix_now_secs();
        let mut entries: Vec<(MsgKey, CachedTranscript)> = disk
            .entries
            .into_iter()
            .filter_map(|(raw_key, entry)| {
                let age = now.saturating_sub(entry.updated_at_secs);
                if age > CACHE_MAX_AGE_SECS {
                    return None;
                }
                let key = MsgKey::from_cache_key(&raw_key)?;
                Some((key, entry))
            })
            .collect();
        prune_entries(&mut entries, CACHE_MAX_ENTRIES, CACHE_MAX_AGE_SECS, now);
        for (key, entry) in entries {
            self.states.insert(key, TranscriptState::Done(entry));
        }
    }

    fn prune_and_save_cache(&mut self) {
        let now = unix_now_secs();
        let mut entries: Vec<(MsgKey, CachedTranscript)> = self
            .states
            .iter()
            .filter_map(|(key, state)| match state {
                TranscriptState::Done(entry) => Some((key.clone(), entry.clone())),
                _ => None,
            })
            .collect();
        let before = entries.len();
        prune_entries(&mut entries, CACHE_MAX_ENTRIES, CACHE_MAX_AGE_SECS, now);
        if entries.len() < before {
            let kept: HashSet<MsgKey> = entries.iter().map(|(k, _)| k.clone()).collect();
            self.states.retain(|key, state| {
                !matches!(state, TranscriptState::Done(_)) || kept.contains(key)
            });
        }

        let mut map = HashMap::with_capacity(entries.len());
        for (key, entry) in entries {
            map.insert(key.cache_key(), entry);
        }
        let file = DiskCacheFile {
            version: 1,
            entries: map,
        };
        let Ok(payload) = serde_json::to_vec(&file) else {
            return;
        };
        let path = self.cache_path.clone();
        let _ = thread::Builder::new()
            .name("stt-cache-save".to_owned())
            .spawn(move || {
                if let Some(parent) = path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                let tmp = path.with_extension("json.tmp");
                if fs::write(&tmp, &payload).is_ok() && fs::rename(&tmp, &path).is_err() {
                    let _ = fs::write(&path, &payload);
                    let _ = fs::remove_file(&tmp);
                }
            });
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn prune_entries<K: Clone>(
    entries: &mut Vec<(K, CachedTranscript)>,
    max_entries: usize,
    max_age_secs: u64,
    now_secs: u64,
) {
    entries.retain(|(_, entry)| now_secs.saturating_sub(entry.updated_at_secs) <= max_age_secs);
    if entries.len() > max_entries {
        entries.sort_by_key(|(_, a)| std::cmp::Reverse(a.updated_at_secs));
        entries.truncate(max_entries);
    }
}

#[derive(Debug)]
struct ApiError {
    message: String,
    auth_failed: bool,
}

impl ApiError {
    fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            auth_failed: false,
        }
    }

    fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            auth_failed: false,
        }
    }
}

fn build_http_client(
    proxy: Option<&reqwest::Proxy>,
    timeout_secs: u64,
) -> Result<reqwest::blocking::Client, String> {
    let mut builder = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .user_agent("zapfast-stt/1.0");
    if let Some(proxy) = proxy {
        builder = builder.proxy(proxy.clone());
    }
    builder
        .build()
        .map_err(|err| format!("Errore client HTTP: {err}"))
}

fn run_transcription(
    path: &Path,
    mime: &str,
    config: &SttConfig,
) -> Result<CachedTranscript, String> {
    let metadata =
        fs::metadata(path).map_err(|err| format!("Impossibile leggere il file audio: {err}"))?;
    let file_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if file_len == 0 {
        return Err("File audio vuoto (0 byte).".to_owned());
    }

    let bytes =
        fs::read(path).map_err(|err| format!("Impossibile leggere il file audio: {err}"))?;
    let clean_mime = sanitize_audio_mime(mime, path);
    let filename = audio_filename(path, &clean_mime);
    let client = build_http_client(config.proxy.as_ref(), HTTP_TIMEOUT_SECS)?;

    let has_groq = !config.groq_api_key.is_empty();
    let has_google = !config.google_api_key.is_empty();

    if config.debug {
        log::info!(
            "stt start provider={} model={} lang={} mime={} bytes={}",
            config.provider.as_str(),
            config.model,
            config.language,
            clean_mime,
            bytes.len()
        );
    }

    match config.provider {
        SttProvider::Groq => {
            if !has_groq {
                return Err("Chiave API Groq mancante nelle Impostazioni.".to_owned());
            }
            transcribe_groq(&client, &bytes, &filename, &clean_mime, config)
                .map_err(|err| err.message)
        }
        SttProvider::Google => {
            if !has_google {
                return Err("Chiave API Google Gemini mancante nelle Impostazioni.".to_owned());
            }
            transcribe_google(&client, &bytes, &clean_mime, config).map_err(|err| err.message)
        }
        SttProvider::Auto => {
            if !has_groq && !has_google {
                return Err(
                    "Configura almeno una chiave API (Groq o Google Gemini) in Impostazioni -> Trascrizione vocale."
                        .to_owned(),
                );
            }
            if has_groq {
                match transcribe_groq(&client, &bytes, &filename, &clean_mime, config) {
                    Ok(transcript) => return Ok(transcript),
                    Err(groq_err) => {
                        if config.debug {
                            log::warn!("stt groq failed, trying fallback: {}", groq_err.message);
                        }
                        if has_google {
                            match transcribe_google(&client, &bytes, &clean_mime, config) {
                                Ok(mut transcript) => {
                                    transcript.provider = "Google (fallback)".to_owned();
                                    return Ok(transcript);
                                }
                                Err(google_err) => {
                                    return Err(format!(
                                        "Groq fallito ({}) · Google fallito ({})",
                                        groq_err.message, google_err.message
                                    ));
                                }
                            }
                        }
                        return Err(groq_err.message);
                    }
                }
            }
            transcribe_google(&client, &bytes, &clean_mime, config).map_err(|err| err.message)
        }
    }
}

fn transcribe_groq(
    client: &reqwest::blocking::Client,
    bytes: &[u8],
    filename: &str,
    mime: &str,
    config: &SttConfig,
) -> Result<CachedTranscript, ApiError> {
    if bytes.len() > MAX_GROQ_UPLOAD_BYTES {
        return Err(ApiError::fatal(format!(
            "Audio troppo grande per Groq ({:.1} MB > 25 MB).",
            bytes.len() as f64 / (1024.0 * 1024.0)
        )));
    }

    let boundary = random_boundary();
    let lang = if config.language.eq_ignore_ascii_case("auto") {
        None
    } else {
        Some(config.language.as_str())
    };
    let body = build_groq_multipart_body(&boundary, bytes, filename, mime, &config.model, lang);
    let content_type = format!("multipart/form-data; boundary={boundary}");

    let mut last_err = ApiError::fatal("Errore sconosciuto Groq.");
    for attempt in 0..GROQ_MAX_ATTEMPTS {
        let response = client
            .post(GROQ_URL)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", config.groq_api_key),
            )
            .header(reqwest::header::CONTENT_TYPE, &content_type)
            .body(body.clone())
            .send();

        match response {
            Ok(resp) => {
                let status = resp.status();
                let retry_after = parse_retry_after(resp.headers());
                let raw_body = resp.text().unwrap_or_default();
                if status.is_success() {
                    let parsed: GroqResponse = serde_json::from_str(&raw_body).map_err(|err| {
                        ApiError::fatal(format!("Risposta Groq non valida: {err}"))
                    })?;
                    let text = normalize_transcript_text(&parsed.text);
                    if config.debug {
                        log::info!("stt groq ok chars={}", text.len());
                    }
                    return Ok(CachedTranscript {
                        text,
                        provider: "Groq".to_owned(),
                        model: config.model.clone(),
                        language: config.language.clone(),
                        updated_at_secs: unix_now_secs(),
                    });
                }

                let detail = extract_api_error_message(&raw_body);
                let code = status.as_u16();
                if code == 401 || code == 403 {
                    return Err(ApiError {
                        message: format!(
                            "Chiave API Groq non valida o autorizzazione negata (HTTP {code})."
                        ),
                        auth_failed: true,
                    });
                }
                let retryable = code == 429 || code >= 500;
                last_err = ApiError {
                    message: format!("Groq HTTP {code}: {detail}"),
                    auth_failed: false,
                };
                if retryable && attempt + 1 < GROQ_MAX_ATTEMPTS {
                    let delay = retry_after.unwrap_or_else(|| 1u64 << attempt).min(20);
                    thread::sleep(Duration::from_secs(delay));
                    continue;
                }
                return Err(last_err);
            }
            Err(err) => {
                last_err = ApiError::retryable(format!("Errore rete Groq: {err}"));
                if attempt + 1 < GROQ_MAX_ATTEMPTS {
                    let delay = (1u64 << attempt).min(16);
                    thread::sleep(Duration::from_secs(delay));
                    continue;
                }
                return Err(last_err);
            }
        }
    }
    Err(last_err)
}

fn transcribe_google(
    client: &reqwest::blocking::Client,
    bytes: &[u8],
    mime: &str,
    config: &SttConfig,
) -> Result<CachedTranscript, ApiError> {
    if bytes.len() > MAX_GOOGLE_INLINE_BYTES {
        return Err(ApiError::fatal(format!(
            "Audio troppo grande per Google inline ({:.1} MB > 14 MB).",
            bytes.len() as f64 / (1024.0 * 1024.0)
        )));
    }

    let encoded_audio = BASE64_STANDARD.encode(bytes);
    let prompt = build_google_prompt(&config.language);
    let request_json = GeminiRequest {
        contents: vec![GeminiContent {
            parts: vec![
                GeminiPart::Text { text: prompt },
                GeminiPart::InlineData {
                    inline_data: GeminiInlineData {
                        mime_type: mime.to_owned(),
                        data: encoded_audio,
                    },
                },
            ],
        }],
        generation_config: GeminiGenerationConfig { temperature: 0.0 },
    };
    let payload = serde_json::to_vec(&request_json)
        .map_err(|err| ApiError::fatal(format!("Serializzazione JSON Gemini fallita: {err}")))?;

    let mut last_err = ApiError::fatal("Nessun modello Google Gemini disponibile.");
    for &model in GOOGLE_MODELS {
        let url = format!("{GEMINI_BASE_URL}/{model}:generateContent");
        for attempt in 0..GEMINI_ATTEMPTS_PER_MODEL {
            let response = client
                .post(&url)
                .header("x-goog-api-key", &config.google_api_key)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(payload.clone())
                .send();

            match response {
                Ok(resp) => {
                    let status = resp.status();
                    let retry_after = parse_retry_after(resp.headers());
                    let raw_body = resp.text().unwrap_or_default();
                    if status.is_success() {
                        let text = extract_gemini_text(&raw_body)?;
                        if config.debug {
                            log::info!("stt google ok model={} chars={}", model, text.len());
                        }
                        return Ok(CachedTranscript {
                            text,
                            provider: "Google".to_owned(),
                            model: model.to_owned(),
                            language: config.language.clone(),
                            updated_at_secs: unix_now_secs(),
                        });
                    }

                    let code = status.as_u16();
                    let detail = extract_api_error_message(&raw_body);
                    let auth_failed = code == 401
                        || code == 403
                        || (code == 400
                            && detail.to_ascii_lowercase().contains("api key not valid"));
                    if auth_failed {
                        return Err(ApiError {
                            message: format!(
                                "Chiave API Google Gemini non valida (HTTP {code}): {detail}"
                            ),
                            auth_failed: true,
                        });
                    }

                    let model_unavailable = code == 404;
                    let retryable = code == 429 || code >= 500;
                    last_err = ApiError {
                        message: format!("Google ({model}) HTTP {code}: {detail}"),
                        auth_failed: false,
                    };
                    if model_unavailable {
                        break;
                    }
                    if retryable && attempt + 1 < GEMINI_ATTEMPTS_PER_MODEL {
                        let delay = retry_after.unwrap_or(2).min(10);
                        thread::sleep(Duration::from_secs(delay));
                        continue;
                    }
                    // On quota / overload (429/503), fall through to try the next Gemini model.
                    if retryable {
                        break;
                    }
                    return Err(last_err);
                }
                Err(err) => {
                    last_err = ApiError::retryable(format!("Errore rete Google ({model}): {err}"));
                    if attempt + 1 < GEMINI_ATTEMPTS_PER_MODEL {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    break;
                }
            }
        }
        if last_err.auth_failed {
            return Err(last_err);
        }
    }

    Err(last_err)
}

fn verify_groq_key(
    api_key: &str,
    model: &str,
    proxy: Option<&reqwest::Proxy>,
) -> Result<String, String> {
    let client = build_http_client(proxy, KEY_TEST_TIMEOUT_SECS)?;
    let resp = client
        .get(GROQ_MODELS_URL)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {api_key}"))
        .send()
        .map_err(|err| format!("Connessione Groq fallita: {err}"))?;
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    if !status.is_success() {
        let msg = extract_api_error_message(&body);
        return Err(format!("Groq HTTP {}: {msg}", status.as_u16()));
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|err| format!("Risposta Groq non valida: {err}"))?;
    let count = parsed
        .get("data")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    let has_model = parsed
        .get("data")
        .and_then(|v| v.as_array())
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("id").and_then(|id| id.as_str()) == Some(model))
        });
    if has_model {
        Ok(format!("Chiave valida ({count} modelli · {model} pronto)"))
    } else {
        Ok(format!("Chiave valida ({count} modelli disponibili)"))
    }
}

fn verify_google_key(api_key: &str, proxy: Option<&reqwest::Proxy>) -> Result<String, String> {
    let client = build_http_client(proxy, KEY_TEST_TIMEOUT_SECS)?;
    let resp = client
        .get(GEMINI_BASE_URL)
        .header("x-goog-api-key", api_key)
        .send()
        .map_err(|err| format!("Connessione Google fallita: {err}"))?;
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    if !status.is_success() {
        let msg = extract_api_error_message(&body);
        return Err(format!("Google HTTP {}: {msg}", status.as_u16()));
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&body).map_err(|err| format!("Risposta Google non valida: {err}"))?;
    let count = parsed
        .get("models")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);
    Ok(format!("Chiave valida ({count} modelli Gemini)"))
}

#[derive(Deserialize)]
struct GroqResponse {
    text: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiRequest {
    contents: Vec<GeminiContent>,
    generation_config: GeminiGenerationConfig,
}

#[derive(Serialize)]
struct GeminiContent {
    parts: Vec<GeminiPart>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum GeminiPart {
    Text { text: String },
    InlineData { inline_data: GeminiInlineData },
}

#[derive(Serialize)]
struct GeminiInlineData {
    mime_type: String,
    data: String,
}

#[derive(Serialize)]
struct GeminiGenerationConfig {
    temperature: f32,
}

fn build_google_prompt(language: &str) -> String {
    let trimmed = language.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("auto") {
        "Trascrivi fedelmente questo messaggio vocale WhatsApp. Restituisci esclusivamente la trascrizione, senza commenti, titoli o prefissi."
            .to_owned()
    } else {
        let lang_hint = match trimmed.to_ascii_lowercase().as_str() {
            "it" => "italiano (it)",
            "en" => "inglese (en)",
            "es" => "spagnolo (es)",
            "fr" => "francese (fr)",
            "de" => "tedesco (de)",
            "pt" => "portoghese (pt)",
            _ => trimmed,
        };
        format!(
            "Trascrivi fedelmente questo messaggio vocale WhatsApp. Lingua attesa: {lang_hint}. Restituisci esclusivamente la trascrizione, senza commenti, titoli o prefissi."
        )
    }
}

fn extract_gemini_text(raw_body: &str) -> Result<String, ApiError> {
    let value: serde_json::Value = serde_json::from_str(raw_body)
        .map_err(|err| ApiError::fatal(format!("Risposta Gemini non valida: {err}")))?;
    let candidates = value
        .get("candidates")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            ApiError::fatal("Risposta Gemini priva di trascrizione (nessun candidato).")
        })?;
    let mut joined = String::new();
    if let Some(first) = candidates.first()
        && let Some(parts) = first
            .get("content")
            .and_then(|c| c.get("parts"))
            .and_then(|p| p.as_array())
    {
        for part in parts {
            if let Some(piece) = part.get("text").and_then(|t| t.as_str()) {
                if !joined.is_empty() && !piece.starts_with(char::is_whitespace) {
                    joined.push(' ');
                }
                joined.push_str(piece);
            }
        }
    }
    Ok(normalize_transcript_text(&joined))
}

fn normalize_transcript_text(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "(Nessun parlato rilevato)".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn extract_api_error_message(raw_body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(raw_body)
        && let Some(message) = value
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
    {
        let trimmed = message.trim();
        if !trimmed.is_empty() {
            return trimmed.chars().take(180).collect();
        }
    }
    let snippet: String = raw_body.trim().chars().take(140).collect();
    if snippet.is_empty() {
        "nessun dettaglio".to_owned()
    } else {
        snippet
    }
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    raw.trim().parse::<u64>().ok()
}

pub fn sanitize_audio_mime(raw_mime: &str, path: &Path) -> String {
    let base = raw_mime
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match base.as_str() {
        "audio/opus" => "audio/ogg".to_owned(),
        "audio/x-wav" => "audio/wav".to_owned(),
        m if m.starts_with("audio/") => m.to_owned(),
        _ => match path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase())
            .as_deref()
        {
            Some("ogg") | Some("oga") | Some("opus") => "audio/ogg".to_owned(),
            Some("mp3") => "audio/mpeg".to_owned(),
            Some("m4a") | Some("mp4") => "audio/mp4".to_owned(),
            Some("wav") => "audio/wav".to_owned(),
            Some("webm") => "audio/webm".to_owned(),
            Some("aac") => "audio/aac".to_owned(),
            Some("flac") => "audio/flac".to_owned(),
            _ => "audio/ogg".to_owned(),
        },
    }
}

fn audio_filename(path: &Path, mime: &str) -> String {
    let ext = match mime {
        "audio/mpeg" => "mp3",
        "audio/mp4" => "m4a",
        "audio/wav" => "wav",
        "audio/webm" => "webm",
        "audio/aac" => "aac",
        "audio/flac" => "flac",
        _ => "ogg",
    };
    if let Some(stem) = path
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| {
            s.chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                .take(32)
                .collect::<String>()
        })
        .filter(|s| !s.is_empty())
    {
        format!("{stem}.{ext}")
    } else {
        format!("voice.{ext}")
    }
}

fn random_boundary() -> String {
    let mut bytes = [0u8; 16];
    let rng = SystemRandom::new();
    if rng.fill(&mut bytes).is_err() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        return format!("----ZapFastSttBoundary{nanos:032x}");
    }
    format!("----ZapFastSttBoundary{}", hex(&bytes))
}

pub fn build_groq_multipart_body(
    boundary: &str,
    audio_bytes: &[u8],
    filename: &str,
    mime: &str,
    model: &str,
    language: Option<&str>,
) -> Vec<u8> {
    let safe_filename = filename.replace(['"', '\r', '\n'], "_");
    let safe_mime = mime.replace(['"', '\r', '\n'], "");
    let mut body = Vec::with_capacity(audio_bytes.len() + 1024);

    push_multipart_file_header(&mut body, boundary, "file", &safe_filename, &safe_mime);
    body.extend_from_slice(audio_bytes);
    body.extend_from_slice(b"\r\n");

    push_multipart_text(&mut body, boundary, "model", model);
    push_multipart_text(&mut body, boundary, "temperature", "0");
    push_multipart_text(&mut body, boundary, "response_format", "json");
    if let Some(lang) = language.map(str::trim).filter(|s| !s.is_empty()) {
        push_multipart_text(&mut body, boundary, "language", lang);
    }

    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary.as_bytes());
    body.extend_from_slice(b"--\r\n");
    body
}

fn push_multipart_file_header(
    out: &mut Vec<u8>,
    boundary: &str,
    field: &str,
    filename: &str,
    mime: &str,
) {
    out.extend_from_slice(b"--");
    out.extend_from_slice(boundary.as_bytes());
    out.extend_from_slice(b"\r\nContent-Disposition: form-data; name=\"");
    out.extend_from_slice(field.as_bytes());
    out.extend_from_slice(b"\"; filename=\"");
    out.extend_from_slice(filename.as_bytes());
    out.extend_from_slice(b"\"\r\nContent-Type: ");
    out.extend_from_slice(mime.as_bytes());
    out.extend_from_slice(b"\r\n\r\n");
}

fn push_multipart_text(out: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    out.extend_from_slice(b"--");
    out.extend_from_slice(boundary.as_bytes());
    out.extend_from_slice(b"\r\nContent-Disposition: form-data; name=\"");
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(b"\"\r\n\r\n");
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn count_audio_messages(messages: &[Message]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m.content, Content::Audio { .. }))
        .count()
}

pub fn ui_header_width(ui: &Ui, stt: &SttEngine, chat_id: &str, has_audio: bool) -> f32 {
    if !has_audio {
        return 0.0;
    }
    let bulk = stt.bulk();
    let pill_label = if bulk.active && bulk.chat_id == chat_id {
        format!("Stop ({}/{})", bulk.done, bulk.total)
    } else {
        "Trascrivi tutti".to_owned()
    };
    let pill = theme::soft_button_width(ui, &pill_label, true);
    let gear = 16.0 + theme::ICON_BUTTON_PAD;
    let gap = ui.spacing().item_spacing.x;
    pill + gear + gap * 2.0
}

pub fn ui_header_controls(
    ui: &mut Ui,
    stt: &SttEngine,
    chat_id: &str,
    has_audio: bool,
    palette: &Palette,
    actions: &mut Vec<Action>,
) {
    if !has_audio {
        return;
    }
    let gear = theme::icon_button(
        ui,
        Icon::Settings,
        16.0,
        palette.secondary,
        palette.text,
        "Impostazioni trascrizione vocale",
    );
    if gear.clicked() {
        actions.push(Action::OpenSttSettings);
    }

    let bulk = stt.bulk();
    let is_this_chat = bulk.active && bulk.chat_id == chat_id;
    let label = if is_this_chat {
        format!("Stop ({}/{})", bulk.done, bulk.total)
    } else {
        "Trascrivi tutti".to_owned()
    };
    let pill = theme::soft_button(
        ui,
        palette,
        Some(if is_this_chat { Icon::X } else { Icon::Mic }),
        &label,
        is_this_chat,
    );
    if pill.clicked() {
        if is_this_chat {
            actions.push(Action::StopBulkTranscribe);
        } else {
            actions.push(Action::TranscribeAllVoices(chat_id.to_owned()));
        }
    }
}

pub fn ui_voice_bubble(
    ui: &mut Ui,
    stt: &SttEngine,
    palette: &Palette,
    chat_id: &ChatId,
    message: &Message,
    width: f32,
    actions: &mut Vec<Action>,
) {
    let state = stt.state(chat_id, &message.id);
    let box_width = width.clamp(160.0, TRANSCRIPT_MAX_WIDTH);
    let accent = palette.accent;
    let subtle_border = palette.outline;
    let card_bg = palette.surface.lerp_to_gamma(palette.surface_active, 0.25);

    ui.allocate_ui_with_layout(
        egui::vec2(box_width, 0.0),
        Layout::top_down(Align::Min),
        |ui| {
            ui.set_max_width(box_width);
            ui.add_space(4.0);

            ui.horizontal_wrapped(|ui| {
                ui.spacing_mut().item_spacing.x = 6.0;
                match state {
                    TranscriptState::Idle => {
                        if theme::soft_button(ui, palette, Some(Icon::Mic), "Trascrivi", false)
                            .clicked()
                        {
                            actions.push(Action::TranscribeVoice {
                                chat: chat_id.clone(),
                                message: message.id.clone(),
                                force: false,
                            });
                        }
                    }
                    TranscriptState::WaitingDownload | TranscriptState::Pending => {
                        let status_label = if matches!(state, TranscriptState::WaitingDownload) {
                            "Scarico…"
                        } else {
                            "In corso…"
                        };
                        ui.add_enabled_ui(false, |ui| {
                            let _ = theme::soft_button(ui, palette, None, status_label, true);
                        });
                        theme::spinner(ui, 13.0, accent);
                    }
                    TranscriptState::Done(entry) => {
                        if theme::soft_button(ui, palette, Some(Icon::Refresh), "Riprova", false)
                            .clicked()
                        {
                            actions.push(Action::TranscribeVoice {
                                chat: chat_id.clone(),
                                message: message.id.clone(),
                                force: true,
                            });
                        }
                        let copied = stt.was_just_copied(chat_id, &message.id, ui.ctx());
                        let copy_label = if copied { "Copiato ✓" } else { "Copia" };
                        let copy_icon = if copied { Icon::Check } else { Icon::Copy };
                        if theme::soft_button(ui, palette, Some(copy_icon), copy_label, copied)
                            .clicked()
                        {
                            actions.push(Action::CopySttTranscript {
                                chat: chat_id.clone(),
                                message: message.id.clone(),
                                text: entry.text.clone(),
                            });
                        }
                        ui.label(
                            RichText::new(format!("{} · {}", entry.provider, entry.model))
                                .font(theme::regular(10.5))
                                .color(palette.secondary),
                        );
                    }
                    TranscriptState::Error(_) => {
                        if theme::soft_button(ui, palette, Some(Icon::Refresh), "Riprova", false)
                            .clicked()
                        {
                            actions.push(Action::TranscribeVoice {
                                chat: chat_id.clone(),
                                message: message.id.clone(),
                                force: true,
                            });
                        }
                    }
                }
            });

            match state {
                TranscriptState::Done(entry) => {
                    ui.add_space(4.0);
                    transcript_card(
                        ui,
                        palette,
                        card_bg,
                        subtle_border,
                        accent,
                        &entry.text,
                        false,
                    );
                }
                TranscriptState::Error(error) => {
                    ui.add_space(4.0);
                    transcript_card(
                        ui,
                        palette,
                        card_bg,
                        subtle_border,
                        palette.danger,
                        error,
                        true,
                    );
                }
                TranscriptState::Idle
                | TranscriptState::WaitingDownload
                | TranscriptState::Pending => {}
            }
        },
    );
}

fn transcript_card(
    ui: &mut Ui,
    palette: &Palette,
    bg: egui::Color32,
    border: egui::Color32,
    bar_color: egui::Color32,
    text: &str,
    is_error: bool,
) {
    let frame = egui::Frame::NONE
        .fill(bg)
        .stroke(Stroke::new(1.0, border))
        .corner_radius(CornerRadius::same(8))
        .inner_margin(Margin::symmetric(10, 8));
    let response = frame.show(ui, |ui| {
        ui.set_min_width((ui.available_width() - 4.0).max(120.0));
        ui.horizontal(|ui| {
            ui.add_space(6.0);
            let color = if is_error {
                palette.danger
            } else {
                palette.text
            };
            let label =
                egui::Label::new(RichText::new(text).font(theme::regular(13.0)).color(color))
                    .wrap()
                    .selectable(true)
                    .sense(egui::Sense::click());
            ui.add(label);
        });
    });

    let rect = response.response.rect;
    let bar_rect = egui::Rect::from_min_max(
        egui::pos2(rect.left(), rect.top() + 2.0),
        egui::pos2(rect.left() + 3.0, rect.bottom() - 2.0),
    );
    ui.painter()
        .rect_filled(bar_rect, CornerRadius::same(2), bar_color);
}

pub fn ui_voice_context_menu(
    ui: &mut Ui,
    stt: &SttEngine,
    palette: &Palette,
    chat_id: &ChatId,
    message: &Message,
    actions: &mut Vec<Action>,
) {
    let state = stt.state(chat_id, &message.id);
    let busy = matches!(
        state,
        TranscriptState::Pending | TranscriptState::WaitingDownload
    );
    let label = match state {
        TranscriptState::Idle => "Trascrivi vocale",
        TranscriptState::Done(_) | TranscriptState::Error(_) => "Ritrascrivi vocale",
        TranscriptState::WaitingDownload | TranscriptState::Pending => "Trascrizione in corso…",
    };
    if widgets::menu_item_enabled(ui, palette, Some(Icon::Mic), label, !busy) {
        actions.push(Action::TranscribeVoice {
            chat: chat_id.clone(),
            message: message.id.clone(),
            force: !matches!(state, TranscriptState::Idle),
        });
        ui.close();
    }
    if let TranscriptState::Done(entry) = state
        && widgets::menu_item(ui, palette, Some(Icon::Copy), "Copia trascrizione")
    {
        actions.push(Action::CopySttTranscript {
            chat: chat_id.clone(),
            message: message.id.clone(),
            text: entry.text.clone(),
        });
        ui.close();
    }
}

pub fn ui_settings_keys_block(
    ui: &mut Ui,
    stt: &mut SttEngine,
    settings: &mut Settings,
    palette: &Palette,
    actions: &mut Vec<Action>,
) {
    ui.spacing_mut().item_spacing.y = 10.0;

    key_editor_row(
        ui,
        palette,
        KeyEditorSpec {
            kind: KeyKind::Groq,
            title: "Chiave API Groq (gratuita su console.groq.com)",
            hint: "gsk_...",
        },
        KeyRowState {
            saved_key: &mut settings.stt_groq_api_key,
            draft_slot: &mut stt.settings_draft.groq,
            reveal: &mut stt.settings_draft.reveal_groq,
            test_state: &stt.groq_test,
        },
        actions,
    );

    ui.separator();

    key_editor_row(
        ui,
        palette,
        KeyEditorSpec {
            kind: KeyKind::Google,
            title: "Chiave API Google Gemini (fallback gratuito su aistudio.google.com)",
            hint: "AIza...",
        },
        KeyRowState {
            saved_key: &mut settings.stt_google_api_key,
            draft_slot: &mut stt.settings_draft.google,
            reveal: &mut stt.settings_draft.reveal_google,
            test_state: &stt.google_test,
        },
        actions,
    );

    ui.separator();

    let count = stt.cache_count();
    ui.horizontal_wrapped(|ui| {
        ui.label(
            RichText::new(format!(
                "Trascrizioni in cache: {count} / {CACHE_MAX_ENTRIES} (TTL 90 gg)"
            ))
            .font(theme::regular(13.0))
            .color(palette.secondary),
        );
        ui.add_space(8.0);
        ui.add_enabled_ui(count > 0, |ui| {
            if theme::soft_button(ui, palette, Some(Icon::X), "Svuota cache", false).clicked() {
                actions.push(Action::ClearSttCache);
            }
        });
    });
}

struct KeyEditorSpec<'a> {
    kind: KeyKind,
    title: &'a str,
    hint: &'a str,
}

struct KeyRowState<'a> {
    saved_key: &'a mut String,
    draft_slot: &'a mut Option<String>,
    reveal: &'a mut bool,
    test_state: &'a KeyTestState,
}

fn key_editor_row(
    ui: &mut Ui,
    palette: &Palette,
    spec: KeyEditorSpec<'_>,
    state: KeyRowState<'_>,
    actions: &mut Vec<Action>,
) {
    let KeyRowState {
        saved_key,
        draft_slot,
        reveal,
        test_state,
    } = state;
    if draft_slot.is_none() {
        *draft_slot = Some(saved_key.clone());
    }
    let Some(draft) = draft_slot.as_mut() else {
        return;
    };

    ui.label(
        RichText::new(spec.title)
            .font(theme::medium(13.5))
            .color(palette.text),
    );

    ui.horizontal(|ui| {
        let input_width = (ui.available_width() - 250.0).max(160.0);
        let edit = egui::TextEdit::singleline(draft)
            .id(Id::new(("stt-key-input", spec.kind as u8)))
            .password(!*reveal)
            .hint_text(spec.hint)
            .desired_width(input_width);
        let response = ui.add(edit);
        theme::reveal_focus(&response);
        if response.changed() {
            *saved_key = draft.trim().to_owned();
            actions.push(Action::SettingsChanged);
        }

        let eye_icon = if *reveal { Icon::EyeOff } else { Icon::Eye };
        let eye_tip = if *reveal {
            "Nascondi chiave"
        } else {
            "Mostra chiave"
        };
        if theme::icon_button(ui, eye_icon, 16.0, palette.secondary, palette.text, eye_tip)
            .clicked()
        {
            *reveal = !*reveal;
        }

        let testing = matches!(test_state, KeyTestState::Testing);
        ui.add_enabled_ui(!testing && !draft.trim().is_empty(), |ui| {
            if theme::pill_button(ui, palette, "Test", true).clicked() {
                *saved_key = draft.trim().to_owned();
                actions.push(Action::SettingsChanged);
                actions.push(Action::TestSttKey(spec.kind));
            }
        });

        ui.add_enabled_ui(!draft.is_empty() || !saved_key.is_empty(), |ui| {
            if theme::soft_button(ui, palette, Some(Icon::X), "Cancella", false).clicked() {
                draft.clear();
                saved_key.clear();
                actions.push(Action::ClearSttKey(spec.kind));
                actions.push(Action::SettingsChanged);
            }
        });
    });

    match test_state {
        KeyTestState::Idle => {}
        KeyTestState::Testing => {
            ui.horizontal(|ui| {
                theme::spinner(ui, 13.0, palette.accent);
                ui.label(
                    RichText::new("Verifica chiave in corso…")
                        .font(theme::regular(12.0))
                        .color(palette.secondary),
                );
            });
        }
        KeyTestState::Ok(msg) => {
            ui.label(
                RichText::new(msg)
                    .font(theme::regular(12.0))
                    .color(palette.accent),
            );
        }
        KeyTestState::Err(err) => {
            ui.label(
                RichText::new(err)
                    .font(theme::regular(12.0))
                    .color(palette.danger),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groq_multipart_contains_expected_parts() {
        let body = build_groq_multipart_body(
            "TESTBOUNDARY",
            b"OggS\x00\x02",
            "voice.ogg",
            "audio/ogg",
            "whisper-large-v3-turbo",
            Some("it"),
        );
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("--TESTBOUNDARY\r\n"));
        assert!(text.contains("name=\"file\"; filename=\"voice.ogg\""));
        assert!(text.contains("Content-Type: audio/ogg"));
        assert!(text.contains("name=\"model\"\r\n\r\nwhisper-large-v3-turbo"));
        assert!(text.contains("name=\"temperature\"\r\n\r\n0"));
        assert!(text.contains("name=\"response_format\"\r\n\r\njson"));
        assert!(text.contains("name=\"language\"\r\n\r\nit"));
        assert!(text.ends_with("--TESTBOUNDARY--\r\n"));
    }

    #[test]
    fn groq_multipart_omits_language_when_none() {
        let body = build_groq_multipart_body(
            "B1",
            b"abc",
            "clip.m4a",
            "audio/mp4",
            "whisper-large-v3-turbo",
            None,
        );
        let text = String::from_utf8_lossy(&body);
        assert!(!text.contains("name=\"language\""));
    }

    #[test]
    fn sanitize_mime_strips_codec_parameters() {
        assert_eq!(
            sanitize_audio_mime("audio/ogg; codecs=opus", Path::new("voice.ogg")),
            "audio/ogg"
        );
        assert_eq!(
            sanitize_audio_mime("audio/opus; charset=binary", Path::new("note.opus")),
            "audio/ogg"
        );
        assert_eq!(
            sanitize_audio_mime("application/octet-stream", Path::new("voice.m4a")),
            "audio/mp4"
        );
    }

    #[test]
    fn gemini_text_extracts_parts_and_handles_empty() {
        let ok_json =
            r#"{"candidates":[{"content":{"parts":[{"text":"Ciao"},{"text":"come stai?"}]}}]}"#;
        assert_eq!(extract_gemini_text(ok_json).unwrap(), "Ciao come stai?");

        let blank_json = r#"{"candidates":[{"content":{"parts":[{"text":"   "}]}}]}"#;
        assert_eq!(
            extract_gemini_text(blank_json).unwrap(),
            "(Nessun parlato rilevato)"
        );
    }

    #[test]
    fn prune_entries_enforces_ttl_and_max_count() {
        let now = 10_000_000;
        let mut entries = vec![
            (
                "expired".to_owned(),
                CachedTranscript {
                    text: "old".to_owned(),
                    provider: "Groq".to_owned(),
                    model: "m".to_owned(),
                    language: "it".to_owned(),
                    updated_at_secs: now - CACHE_MAX_AGE_SECS - 1,
                },
            ),
            (
                "older".to_owned(),
                CachedTranscript {
                    text: "1".to_owned(),
                    provider: "Groq".to_owned(),
                    model: "m".to_owned(),
                    language: "it".to_owned(),
                    updated_at_secs: now - 50,
                },
            ),
            (
                "newest".to_owned(),
                CachedTranscript {
                    text: "2".to_owned(),
                    provider: "Groq".to_owned(),
                    model: "m".to_owned(),
                    language: "it".to_owned(),
                    updated_at_secs: now - 10,
                },
            ),
        ];
        prune_entries(&mut entries, 1, CACHE_MAX_AGE_SECS, now);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "newest");
    }

    #[test]
    fn provider_parsing_handles_all_variants() {
        assert_eq!(SttProvider::from_str_lossy("auto"), SttProvider::Auto);
        assert_eq!(SttProvider::from_str_lossy("Groq"), SttProvider::Groq);
        assert_eq!(SttProvider::from_str_lossy("gemini"), SttProvider::Google);
        assert_eq!(SttProvider::from_str_lossy("unknown"), SttProvider::Auto);
    }
}
