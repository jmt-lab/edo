//! Per-task log file writer.
//!
//! [`Log`] wraps a file handle that captures output for a single build task.
//! It implements [`std::io::Write`] and [`IntoRawFd`](std::os::fd::IntoRawFd)
//! so it can be used as both a Rust writer and a raw file descriptor for
//! child processes.

use super::LogManager;
use super::{ContextResult as Result, error};
use parking_lot::Mutex;
use snafu::ResultExt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::IntoRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A cloneable, thread-safe log file for a single build task.
#[derive(Clone)]
pub struct Log {
    manager: LogManager,
    inner: Arc<Mutex<Inner>>,
}

/// Internal state holding the file path and open file handle for a [`Log`].
pub struct Inner {
    path: PathBuf,
    file: File,
}

impl Log {
    /// Creates a new log file at `path`, opening it in append mode.
    pub fn new<P: AsRef<Path>>(manager: &LogManager, path: P) -> Result<Self> {
        Ok(Self {
            manager: manager.clone(),
            inner: Arc::new(Mutex::new(Inner {
                path: path.as_ref().to_path_buf(),
                file: OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path.as_ref())
                    .context(error::IoSnafu)?,
            })),
        })
    }

    /// Returns a reference to the parent [`LogManager`].
    pub fn root(&self) -> &LogManager {
        &self.manager
    }

    /// Returns the full file path of this log.
    pub fn path(&self) -> PathBuf {
        self.inner.lock().path.clone()
    }

    /// Returns the file name component of the log path.
    pub fn log_name(&self) -> String {
        self.inner
            .lock()
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string()
    }

    /// Writes a section header line to the log file.
    pub fn set_subject(&self, subject: &str) {
        let _ = self
            .inner
            .lock()
            .file
            .write_fmt(format_args!("\n------ {subject} ------\n"));
    }
}

impl Write for Log {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // A log should always be receiving text data so we can operate on it as such
        let mut lock = self.inner.lock();
        lock.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.lock().file.flush()
    }
}

impl IntoRawFd for &Log {
    fn into_raw_fd(self) -> std::os::unix::prelude::RawFd {
        let lock = self.inner.lock();
        let file = lock.file.try_clone().unwrap();
        drop(lock);
        file.into_raw_fd()
    }
}
