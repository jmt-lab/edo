//! Interactive transform executor with error recovery.
//!
//! Handles running a single transform, catching failures, and prompting the
//! user with options to view logs, retry, open a shell, or abort.

use std::fs::read_to_string;
use dialoguer::{Editor, Select};
use snafu::ResultExt;
use tracing_indicatif::suspend_tracing_indicatif;
use super::{Result, error};
use crate::{
    context::{Handle, Log},
    environment::Environment,
    storage::Artifact,
    transform::{Transform, TransformStatus},
};

/// Executes a transform with interactive error recovery.
///
/// Runs the given transform within the provided environment. On failure,
/// prompts the user with options to view logs, retry, open a shell, or quit.
/// On success, uploads the resulting artifact to the build cache.
pub async fn execute(
    log: &Log,
    ctx: &Handle,
    transform: &Transform,
    env: &Environment,
) -> Result<Artifact> {
    #[allow(unused_assignments)]
    let mut result: Result<Artifact> = error::NoRunSnafu {}.fail();
    'transform: loop {
        // Make an attempt
        let attempt_result = transform.transform(log, ctx, env).await;
        match &attempt_result {
            // If the attempt was successful exit out and return the resulting artifact
            TransformStatus::Success(artifact) => {
                result = Ok(artifact.clone());
                break 'transform;
            }
            // If the attempt failed for any reason we need to prompt the user what
            // we should do about it.
            TransformStatus::Retryable(log_file, e) | TransformStatus::Failed(log_file, e) => {
                error!(target: "transform", "transformation failed: {}", e.to_string());
                // Collect the valid options to present the user with
                let mut options = Vec::new();
                if log_file.is_some() {
                    options.push("view log");
                }
                if matches!(attempt_result, TransformStatus::Retryable(..)) {
                    options.push("retry");
                }
                if transform.can_shell() {
                    options.push("shell");
                }
                options.push("quit");
                // IMPORTANT! We need to susppend our progress bars to ask the
                // user for what to do.
                let should_quit = suspend_tracing_indicatif(|| {
                    // Acquire an exclusive lock on the console through
                    // the log manager
                    let console_lock = ctx.log().acquire();
                    'prompt: loop {
                        let index = Select::new()
                            .items(options.as_slice())
                            .default(0)
                            .interact()
                            .context(error::InquireSnafu)?;
                        let ans = options[index];
                        match ans {
                            "view log" => {
                                let log_text = read_to_string(
                                    log_file
                                        .as_ref()
                                        .expect("log_file is Some when 'view log' option is present"),
                                )
                                .context(error::IoSnafu)?;
                                Editor::new().edit(&log_text).context(error::InquireSnafu)?;
                                continue 'prompt;
                            }
                            "shell" => {
                                transform.shell(env)?;
                                continue 'prompt;
                            }
                            "retry" => {
                                return Ok(false);
                            }
                            "quit" => {
                                break 'prompt;
                            }
                            _ => {
                                return Ok(false);
                            }
                        }
                    }
                    drop(console_lock);
                    Ok::<bool, error::SchedulerError>(true)
                })?;
                if should_quit {
                    result = error::PassthroughSnafu {
                        message: e.to_string(),
                    }
                    .fail();
                    break 'transform;
                }
            }
        }
    }
    // Upload the result if we have a build cache setup
    match result {
        Ok(artifact) => {
            ctx.storage().upload_build(artifact.config().id()).await?;
            Ok(artifact)
        }
        Err(e) => Err(e),
    }
}
