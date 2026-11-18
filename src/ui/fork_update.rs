//! Modal view and bilingual labels for fork auto-merge & rebuild updates.

use egui::{Align, CornerRadius, Frame, Layout, Margin, ProgressBar, ScrollArea, Stroke};

use crate::app::App;
use crate::fork_update::ForkUpdateStatus;
use crate::i18n::Locale;
use crate::model::Action;
use crate::theme::{self, Icon};
use crate::updates::DownloadState;

/// True when the fork auto-merge modal should handle `app.show_update`
/// instead of the stock signed-release modal.
pub fn handles_modal(app: &App) -> bool {
    matches!(app.update_download, DownloadState::Idle)
        && (app.fork_update.has_repo() || app.fork_update.is_active())
}

/// Top-bar banner summary for either an active fork merge/build or an
/// available upstream update.
pub struct BannerSpec {
    pub icon: Icon,
    pub text: String,
    pub tint: egui::Color32,
    pub danger: bool,
    pub url: String,
}

pub fn banner_spec(app: &App, release: Option<&crate::updates::Release>) -> Option<BannerSpec> {
    let palette = app.palette;
    let it = app.locale == Locale::Italian;
    match app.fork_update.status() {
        ForkUpdateStatus::Running {
            version,
            step,
            progress,
        } => {
            let pct = progress
                .map(|p| format!(" ({}%)", (p * 100.0).round() as u32))
                .unwrap_or_default();
            let text = if it {
                format!("Aggiornamento ZapFast {version}{pct}: {step}")
            } else {
                format!("Updating ZapFast {version}{pct}: {step}")
            };
            Some(BannerSpec {
                icon: Icon::Download,
                text,
                tint: palette.accent,
                danger: false,
                url: release
                    .or(app.update.as_ref())
                    .map(|u| u.url.clone())
                    .unwrap_or_default(),
            })
        }
        ForkUpdateStatus::Restarting { version } => Some(BannerSpec {
            icon: Icon::CircleCheck,
            text: if it {
                format!("ZapFast {version} pronto (STT attiva) — riavvio automatico...")
            } else {
                format!("ZapFast {version} ready (STT preserved) — restarting...")
            },
            tint: palette.accent,
            danger: false,
            url: release
                .or(app.update.as_ref())
                .map(|u| u.url.clone())
                .unwrap_or_default(),
        }),
        ForkUpdateStatus::Failed { version, .. } => Some(BannerSpec {
            icon: Icon::CircleAlert,
            text: if it {
                format!("Aggiornamento ZapFast {version} non riuscito — apri Dettagli")
            } else {
                format!("ZapFast {version} update failed — open Details")
            },
            tint: palette.danger,
            danger: true,
            url: release
                .or(app.update.as_ref())
                .map(|u| u.url.clone())
                .unwrap_or_default(),
        }),
        ForkUpdateStatus::Idle => {
            let update = release.or(app.update.as_ref())?;
            let text = if it {
                format!(
                    "Nuovo aggiornamento ZapFast {} disponibile — clicca per scaricare e unire (STT preservata)",
                    update.version
                )
            } else {
                format!(
                    "ZapFast {} is available — click to download & merge (STT preserved)",
                    update.version
                )
            };
            Some(BannerSpec {
                icon: Icon::Download,
                text,
                tint: palette.accent,
                danger: false,
                url: update.url.clone(),
            })
        }
    }
}

pub fn draw_banner_actions(app: &mut App, ui: &mut egui::Ui) {
    let palette = app.palette;
    let it = app.locale == Locale::Italian;
    match app.fork_update.status() {
        ForkUpdateStatus::Idle => {
            let primary_label = if it {
                "Scarica e unisci"
            } else {
                "Update & merge"
            };
            if theme::soft_button(ui, &palette, Some(Icon::Download), primary_label, true).clicked()
            {
                app.actions.push(Action::StartForkUpdate);
            }
            let details_label = if it { "Dettagli" } else { "Details" };
            if theme::soft_button(ui, &palette, Some(Icon::Info), details_label, false).clicked() {
                app.actions.push(Action::ShowUpdate);
            }
        }
        ForkUpdateStatus::Running { .. } | ForkUpdateStatus::Restarting { .. } => {
            let label = if it { "Dettagli" } else { "Details" };
            if theme::soft_button(ui, &palette, Some(Icon::Info), label, false).clicked() {
                app.actions.push(Action::ShowUpdate);
            }
        }
        ForkUpdateStatus::Failed { .. } => {
            let retry = if it { "Riprova" } else { "Retry" };
            if theme::soft_button(ui, &palette, Some(Icon::Refresh), retry, true).clicked() {
                app.actions.push(Action::StartForkUpdate);
            }
            let details = if it { "Dettagli" } else { "Details" };
            if theme::soft_button(ui, &palette, Some(Icon::Info), details, false).clicked() {
                app.actions.push(Action::ShowUpdate);
            }
        }
    }
}

pub fn show(app: &mut App, ctx: &egui::Context) {
    if !app.show_update {
        return;
    }
    let release = app.update.clone();
    if release.is_none() && !app.fork_update.is_active() {
        app.actions.push(Action::CloseUpdate);
        return;
    }

    let palette = app.palette;
    let it = app.locale == Locale::Italian;
    let mut close = ctx.input(|input| input.key_pressed(egui::Key::Escape));

    let frame = Frame::new()
        .fill(palette.overlay)
        .stroke(Stroke::new(1.0, palette.outline))
        .corner_radius(CornerRadius::same(theme::RADIUS + 4))
        .inner_margin(Margin::same(22))
        .shadow(palette.modal_shadow());

    let title = if it {
        "Aggiorna ZapFast (Fork + STT)"
    } else {
        "Update ZapFast (Fork + STT)"
    };

    egui::Window::new(title)
        .id(egui::Id::new("zapfast-fork-update"))
        .title_bar(false)
        .resizable(false)
        .auto_sized()
        .frame(frame)
        .default_width(480.0)
        .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .show(ctx, |ui| {
            ui.set_width(480.0_f32.min((ctx.content_rect().width() - 64.0).max(260.0)));
            ui.horizontal(|ui| {
                theme::text(ui, title, theme::bold(20.0), palette.text);
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    let close_label = if it { "Chiudi" } else { "Close" };
                    close |= theme::icon_button(
                        ui,
                        Icon::X,
                        18.0,
                        palette.secondary,
                        palette.text,
                        close_label,
                    )
                    .clicked();
                });
            });

            let target_version = match (&release, app.fork_update.status()) {
                (Some(r), _) => r.version.as_str(),
                (
                    None,
                    ForkUpdateStatus::Running { version, .. }
                    | ForkUpdateStatus::Restarting { version }
                    | ForkUpdateStatus::Failed { version, .. },
                ) => version.as_str(),
                (None, ForkUpdateStatus::Idle) => env!("CARGO_PKG_VERSION"),
            };

            ui.add_space(4.0);
            theme::text(
                ui,
                format!("{} -> {}", env!("CARGO_PKG_VERSION"), target_version),
                theme::regular(14.0),
                palette.secondary,
            );
            ui.add_space(10.0);

            let explanation = if it {
                "Scarica la nuova release ufficiale da crmne/zapfast, esegue automaticamente il merge mantenendo la trascrizione vocale STT, compila la build release, aggiorna il tuo fork su GitHub e riavvia ZapFast."
            } else {
                "Fetches the new upstream release from crmne/zapfast, merges cleanly while keeping native STT voice transcription, builds the release binary, pushes to your fork, and restarts ZapFast."
            };
            theme::paragraph(ui, explanation, theme::regular(13.0), palette.secondary);

            if let Some(repo_dir) = app.fork_update.repo_dir() {
                ui.add_space(8.0);
                theme::paragraph(
                    ui,
                    format!("Repo: {}", repo_dir.display()),
                    egui::FontId::monospace(11.5),
                    palette.secondary,
                );
            }

            ui.add_space(14.0);

            match app.fork_update.status().clone() {
                ForkUpdateStatus::Idle => {
                    ui.horizontal_wrapped(|ui| {
                        let start_label = if it {
                            "Scarica, unisci e aggiorna"
                        } else {
                            "Download, merge & update"
                        };
                        if app.fork_update.has_repo()
                            && theme::pill_button(ui, &palette, start_label, true).clicked()
                        {
                            app.actions.push(Action::StartForkUpdate);
                        }
                        if let Some(rel) = &release {
                            let gh_label = if it {
                                "Apri release su GitHub"
                            } else {
                                "Open GitHub release"
                            };
                            if theme::soft_button(
                                ui,
                                &palette,
                                Some(Icon::ExternalLink),
                                gh_label,
                                false,
                            )
                            .clicked()
                            {
                                app.actions.push(Action::OpenUrl(rel.url.clone()));
                            }
                        }
                    });
                }
                ForkUpdateStatus::Running { step, progress, .. } => {
                    theme::paragraph(ui, step, theme::medium(13.5), palette.text);
                    ui.add_space(8.0);
                    let fraction = progress.unwrap_or(0.25).clamp(0.0, 1.0);
                    ui.add(
                        ProgressBar::new(fraction)
                            .fill(palette.accent)
                            .desired_width(ui.available_width()),
                    );
                    draw_log_box(app, ui, palette);
                }
                ForkUpdateStatus::Restarting { .. } => {
                    let msg = if it {
                        "Aggiornamento completato! Chiusura e riavvio automatico di ZapFast in corso..."
                    } else {
                        "Update complete! Closing and restarting ZapFast automatically..."
                    };
                    theme::paragraph(ui, msg, theme::medium(13.5), palette.accent);
                    draw_log_box(app, ui, palette);
                }
                ForkUpdateStatus::Failed { error, .. } => {
                    Frame::new()
                        .fill(palette.danger.gamma_multiply(0.12))
                        .stroke(Stroke::new(1.0, palette.danger.gamma_multiply(0.4)))
                        .corner_radius(CornerRadius::same(theme::RADIUS))
                        .inner_margin(Margin::same(10))
                        .show(ui, |ui| {
                            theme::paragraph(ui, error, theme::regular(12.5), palette.danger);
                        });
                    draw_log_box(app, ui, palette);
                    ui.add_space(12.0);
                    ui.horizontal_wrapped(|ui| {
                        let retry_label = if it {
                            "Riprova aggiornamento"
                        } else {
                            "Retry update"
                        };
                        if app.fork_update.has_repo()
                            && theme::pill_button(ui, &palette, retry_label, true).clicked()
                        {
                            app.actions.push(Action::StartForkUpdate);
                        }
                        if let Some(rel) = &release {
                            let gh_label = if it {
                                "Apri release su GitHub"
                            } else {
                                "Open GitHub release"
                            };
                            if theme::soft_button(
                                ui,
                                &palette,
                                Some(Icon::ExternalLink),
                                gh_label,
                                false,
                            )
                            .clicked()
                            {
                                app.actions.push(Action::OpenUrl(rel.url.clone()));
                            }
                        }
                    });
                }
            }
        });

    if close {
        app.actions.push(Action::CloseUpdate);
    }
}

fn draw_log_box(app: &App, ui: &mut egui::Ui, palette: theme::Palette) {
    let logs: Vec<&str> = app.fork_update.logs().collect();
    if logs.is_empty() {
        return;
    }
    ui.add_space(10.0);
    Frame::new()
        .fill(palette.surface)
        .stroke(Stroke::new(1.0, palette.outline))
        .corner_radius(CornerRadius::same(theme::RADIUS))
        .inner_margin(Margin::same(8))
        .show(ui, |ui| {
            ScrollArea::vertical()
                .max_height(150.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    for line in logs {
                        theme::paragraph(
                            ui,
                            line,
                            egui::FontId::monospace(11.0),
                            palette.secondary,
                        );
                    }
                });
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::AppDirs;
    use crate::settings::Settings;
    use crate::updates::Release;

    #[test]
    fn banner_spec_renders_idle_update_for_available_release() {
        let dir = tempfile::tempdir().unwrap();
        let (mut app, _tx) = App::headless(AppDirs::under(dir.path()), Settings::default());
        let release = Release {
            version: "99.0.0".to_owned(),
            url: "https://github.com/crmne/zapfast/releases/tag/v99.0.0".to_owned(),
            published_at: None,
        };
        app.update = Some(release.clone());
        let spec = banner_spec(&app, Some(&release)).expect("banner spec for release");
        assert!(spec.text.contains("99.0.0"));
        assert!(!spec.danger);
        assert_eq!(spec.url, release.url);
    }

    #[test]
    fn handles_modal_defers_to_stock_when_stock_download_active() {
        let dir = tempfile::tempdir().unwrap();
        let (mut app, _tx) = App::headless(AppDirs::under(dir.path()), Settings::default());
        app.update_download = DownloadState::Downloading {
            received: 10,
            total: 100,
        };
        assert!(!handles_modal(&app));
    }
}
