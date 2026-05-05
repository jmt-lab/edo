use super::Environment;
use super::{EnvResult, error};
use crate::context::Log;
use crate::storage::Id;
use handlebars::Handlebars;
use snafu::{ResultExt, ensure};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

/// A Command represents a delayed series of commands to run inside of an environment.
///
/// All transforms should use this to define how they work.
#[derive(Clone)]
pub struct Command {
    id: Id,
    env: Environment,
    log: Log,
    interpreter: String,
    commands: Vec<String>,
    variables: HashMap<String, String>,
}

impl Command {
    /// Create a new empty command bound to `env` with `bash` as the default interpreter.
    pub fn new(log: &Log, id: &Id, env: &Environment) -> Self {
        Self {
            id: id.clone(),
            env: env.clone(),
            log: log.clone(),
            interpreter: "bash".into(),
            commands: Vec::new(),
            variables: HashMap::new(),
        }
    }

    /// Override the shebang interpreter used when the script is rendered (defaults to `bash`).
    pub fn set_interpreter(&mut self, interpreter: &str) {
        self.interpreter = interpreter.to_string();
    }

    /// Set a handlebars template variable, itself resolved against previously-set variables.
    pub fn set(&mut self, key: &str, value: &str) -> EnvResult<()> {
        let value = self.sub(value)?;
        self.variables.insert(key.to_string(), value);
        Ok(())
    }

    fn sub(&self, line: &str) -> EnvResult<String> {
        let current = line.to_string();
        let hg = Handlebars::new();

        hg.render_template(current.as_str(), &self.variables)
            .context(error::TemplateSnafu)
    }

    /// Append a `cd <path>` step to the script, substituting variables in `path`.
    pub fn chdir(&mut self, path: &str) -> EnvResult<()> {
        self.commands.push(format!("cd {}", self.sub(path)?));
        Ok(())
    }

    /// Append a `pushd <path>` step to the script, substituting variables in `path`.
    pub fn pushd(&mut self, path: &str) -> EnvResult<()> {
        self.commands.push(format!("pushd {}", self.sub(path)?));
        Ok(())
    }

    /// Append a `popd` step to the script.
    pub fn popd(&mut self) {
        self.commands.push("popd".into());
    }

    /// Create a directory and bind its canonical in-environment path to `key` as a template variable.
    pub async fn create_named_dir(&mut self, key: &str, path: &str) -> EnvResult<()> {
        let path = self.sub(path)?;
        let result = self.env.expand(Path::new(path.as_str())).await?;
        self.variables
            .insert(key.to_string(), result.to_string_lossy().to_string());
        self.commands.push(format!("mkdir -p {path}"));
        Ok(())
    }

    /// Append a `mkdir -p <path>` step, substituting variables in `path`.
    pub async fn create_dir(&mut self, path: &str) -> EnvResult<()> {
        let path = self.sub(path)?;
        self.commands.push(format!("mkdir -p {path}"));
        Ok(())
    }

    /// Append a recursive directory removal (`rm -r <path>`) step.
    pub async fn remove_dir(&mut self, path: &str) -> EnvResult<()> {
        let path = self.sub(path)?;
        self.commands.push(format!("rm -r {path}"));
        Ok(())
    }

    /// Append a file removal (`rm <path>`) step.
    pub async fn remove_file(&mut self, path: &str) -> EnvResult<()> {
        let path = self.sub(path)?;
        self.commands.push(format!("rm {path}"));
        Ok(())
    }

    /// Append a move step (`mv <from> <to>`), substituting variables in both paths.
    pub async fn mv(&mut self, from: &str, to: &str) -> EnvResult<()> {
        let from = self.sub(from)?;
        let to = self.sub(to)?;
        self.commands.push(format!("mv {from} {to}"));
        Ok(())
    }

    /// Append a recursive copy step (`cp -r <from> <to>`), substituting variables in both paths.
    pub async fn copy(&mut self, from: &str, to: &str) -> EnvResult<()> {
        let from = self.sub(from)?;
        let to = self.sub(to)?;
        self.commands.push(format!("cp -r {from} {to}"));
        Ok(())
    }

    /// Append a raw command line to the script, substituting variables first.
    pub async fn run(&mut self, cmd: &str) -> EnvResult<()> {
        let cmd = self.sub(cmd)?;
        self.commands.push(cmd);
        Ok(())
    }

    /// Dispatch the assembled script to the bound environment, executing it at `path`.
    ///
    /// Returns an error if the environment reports a non-success exit status.
    pub async fn send(&self, path: &str) -> EnvResult<()> {
        let path = self.sub(path)?;
        let dir = self.env.expand(Path::new(path.as_str())).await?;
        let status = self.env.run(&self.log, &self.id, &dir, self).await?;
        ensure!(status, error::RunSnafu);
        Ok(())
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            r#"#!/usr/bin/env {}
{}"#,
            self.interpreter,
            self.commands.join("\n")
        ))
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for [`Command`].
    //!
    //! Mock [`EnvironmentImpl`] is inlined per-module (matching the pattern in
    //! `scheduler/graph.rs` and `scheduler/execute.rs`) — we deliberately do
    //! not extract a shared `test_support.rs`.  We assert behaviour only via
    //! the `Display` output of a `Command` and via the arguments observed by
    //! the mock's `run()` / `expand()` implementations, so tests stay coupled
    //! to the public contract.
    use super::*;
    use crate::context::test_support::shared_log_manager;
    use crate::environment::error::EnvironmentError;
    use crate::environment::EnvironmentImpl;
    use crate::storage::{Id, Storage};
    use crate::util::{Reader, Writer};
    use async_trait::async_trait;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    /// Configurable `EnvironmentImpl` used by the command tests.
    ///
    /// * `expand_prefix` — when `Some`, `expand(p)` returns `prefix.join(p)`
    ///   (stripping any leading `/` from `p`); when `None`, it echoes `p`
    ///   unchanged.
    /// * `expand_fail` — when `true`, `expand` returns an
    ///   `EnvironmentError::Implementation` error.
    /// * `run_status` — the boolean success flag returned from `run`.
    /// * `runs` — shared log of `(path, command_display)` tuples observed
    ///   by `run()`.
    struct MockEnvImpl {
        expand_prefix: Option<PathBuf>,
        expand_fail: bool,
        run_status: bool,
        runs: Arc<Mutex<Vec<(PathBuf, String)>>>,
    }

    impl MockEnvImpl {
        fn new() -> (Self, Arc<Mutex<Vec<(PathBuf, String)>>>) {
            let runs = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    expand_prefix: None,
                    expand_fail: false,
                    run_status: true,
                    runs: runs.clone(),
                },
                runs,
            )
        }

        fn with_prefix(mut self, prefix: PathBuf) -> Self {
            self.expand_prefix = Some(prefix);
            self
        }

        fn with_run_status(mut self, status: bool) -> Self {
            self.run_status = status;
            self
        }

        fn with_expand_fail(mut self) -> Self {
            self.expand_fail = true;
            self
        }
    }

    #[async_trait]
    impl EnvironmentImpl for MockEnvImpl {
        async fn expand(&self, path: &Path) -> EnvResult<PathBuf> {
            if self.expand_fail {
                return Err(EnvironmentError::Implementation {
                    source: Box::new(std::io::Error::other("expand failed")),
                });
            }
            if let Some(prefix) = &self.expand_prefix {
                let rel = path.strip_prefix("/").unwrap_or(path);
                Ok(prefix.join(rel))
            } else {
                Ok(path.to_path_buf())
            }
        }
        async fn create_dir(&self, _path: &Path) -> EnvResult<()> {
            unimplemented!()
        }
        async fn set_env(&self, _k: &str, _v: &str) -> EnvResult<()> {
            unimplemented!()
        }
        async fn get_env(&self, _k: &str) -> Option<String> {
            unimplemented!()
        }
        async fn setup(&self, _log: &Log, _storage: &Storage) -> EnvResult<()> {
            unimplemented!()
        }
        async fn up(&self, _log: &Log) -> EnvResult<()> {
            unimplemented!()
        }
        async fn down(&self, _log: &Log) -> EnvResult<()> {
            unimplemented!()
        }
        async fn clean(&self, _log: &Log) -> EnvResult<()> {
            unimplemented!()
        }
        async fn write(&self, _p: &Path, _r: Reader) -> EnvResult<()> {
            unimplemented!()
        }
        async fn unpack(&self, _p: &Path, _r: Reader) -> EnvResult<()> {
            unimplemented!()
        }
        async fn read(&self, _p: &Path, _w: Writer) -> EnvResult<()> {
            unimplemented!()
        }
        async fn cmd(&self, _log: &Log, _id: &Id, _p: &Path, _c: &str) -> EnvResult<bool> {
            unimplemented!()
        }
        async fn run(
            &self,
            _log: &Log,
            _id: &Id,
            path: &Path,
            command: &Command,
        ) -> EnvResult<bool> {
            self.runs
                .lock()
                .unwrap()
                .push((path.to_path_buf(), command.to_string()));
            Ok(self.run_status)
        }
        fn shell(&self, _p: &Path) -> EnvResult<()> {
            unimplemented!()
        }
    }

    /// Build a fresh `Log` in `dir` using the process-wide shared
    /// `LogManager`.
    async fn make_log(dir: &TempDir, name: &str) -> Log {
        let mgr = shared_log_manager().await;
        let path = dir.path().join(format!("{name}.log"));
        Log::new(&mgr, &path).expect("Log::new")
    }

    fn make_id() -> Id {
        Id::builder()
            .name("cmd-test".to_string())
            .digest("deadbeef".to_string())
            .build()
    }

    fn make_env() -> (Environment, Arc<Mutex<Vec<(PathBuf, String)>>>) {
        let (mock, runs) = MockEnvImpl::new();
        (Environment::new(mock), runs)
    }

    // ── Display / basic construction ────────────────────────────────────────

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn new_defaults() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "new").await;
        let id = make_id();
        let (env, _) = make_env();
        let cmd = Command::new(&log, &id, &env);
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\n");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn set_interpreter_changes_display_shebang() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "interp").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.set_interpreter("sh");
        assert_eq!(cmd.to_string(), "#!/usr/bin/env sh\n");
    }

    // ── Variable substitution ───────────────────────────────────────────────

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn set_substitutes_previous_variables() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "set-sub").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.set("home", "/root").unwrap();
        cmd.set("cfg", "{{home}}/.rc").unwrap();
        cmd.run("echo {{cfg}}").await.unwrap();
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\necho /root/.rc");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn set_undefined_variable_renders_blank() {
        // Handlebars' default (non-strict) mode renders missing keys as "".
        // This test captures that behaviour so a switch to strict mode is
        // caught here rather than in downstream integration.
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "set-blank").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.run("echo {{missing}}").await.unwrap();
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\necho ");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn set_invalid_template_returns_template_error() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "bad-tmpl").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        let err = cmd.set("x", "{{unterminated").unwrap_err();
        assert!(
            matches!(err, EnvironmentError::Template { .. }),
            "expected Template error, got {err:?}"
        );
    }

    // ── chdir / pushd / popd ────────────────────────────────────────────────

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn chdir_pushd_popd_emit_expected_lines() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "dirs").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.chdir("/a").unwrap();
        cmd.pushd("/b").unwrap();
        cmd.popd();
        let rendered = cmd.to_string();
        assert_eq!(rendered, "#!/usr/bin/env bash\ncd /a\npushd /b\npopd");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn chdir_substitutes_template_vars() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "chdir-sub").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.set("d", "/work").unwrap();
        cmd.chdir("{{d}}").unwrap();
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\ncd /work");
    }

    // ── create_named_dir / create_dir / remove_* / mv / copy ────────────────

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn create_named_dir_calls_expand_and_records_path() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "named").await;
        let id = make_id();
        // Use a prefix-style expand() so the bound variable is observable.
        let (mock, _runs) = MockEnvImpl::new();
        let env = Environment::new(mock.with_prefix(PathBuf::from("/sandbox")));
        let mut cmd = Command::new(&log, &id, &env);
        cmd.create_named_dir("root", "/a").await.unwrap();
        cmd.run("cd {{root}}").await.unwrap();
        assert_eq!(
            cmd.to_string(),
            "#!/usr/bin/env bash\nmkdir -p /a\ncd /sandbox/a"
        );
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn create_dir_emits_mkdir() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "mkdir").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.create_dir("/x").await.unwrap();
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\nmkdir -p /x");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn remove_dir_and_remove_file() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "rm").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.remove_dir("/x").await.unwrap();
        cmd.remove_file("/y").await.unwrap();
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\nrm -r /x\nrm /y");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn mv_and_copy() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "mvcp").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.set("src", "/a").unwrap();
        cmd.set("dst", "/b").unwrap();
        cmd.mv("{{src}}", "{{dst}}").await.unwrap();
        cmd.copy("{{src}}", "{{dst}}").await.unwrap();
        assert_eq!(
            cmd.to_string(),
            "#!/usr/bin/env bash\nmv /a /b\ncp -r /a /b"
        );
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn run_appends_substituted_command() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "run-sub").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.set("name", "world").unwrap();
        cmd.run("echo {{name}}").await.unwrap();
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\necho world");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn display_joins_commands_with_newlines() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "disp").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut cmd = Command::new(&log, &id, &env);
        cmd.run("one").await.unwrap();
        cmd.run("two").await.unwrap();
        assert_eq!(cmd.to_string(), "#!/usr/bin/env bash\none\ntwo");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn clone_is_independent() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "clone").await;
        let id = make_id();
        let (env, _) = make_env();
        let mut original = Command::new(&log, &id, &env);
        original.run("one").await.unwrap();
        let mut cloned = original.clone();
        cloned.run("two").await.unwrap();
        // Original must not see the clone's appended command.
        assert_eq!(original.to_string(), "#!/usr/bin/env bash\none");
        assert_eq!(cloned.to_string(), "#!/usr/bin/env bash\none\ntwo");
    }

    // ── send() — dispatches through Environment::run ────────────────────────

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn send_invokes_environment_run_with_expanded_path() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "send").await;
        let id = make_id();
        let (mock, runs) = MockEnvImpl::new();
        let env = Environment::new(mock.with_prefix(PathBuf::from("/sandbox")));
        let mut cmd = Command::new(&log, &id, &env);
        cmd.run("echo hi").await.unwrap();
        cmd.send("/script").await.expect("send ok");

        let log = runs.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].0, PathBuf::from("/sandbox/script"));
        assert_eq!(log[0].1, "#!/usr/bin/env bash\necho hi");
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn send_returns_run_error_on_false_status() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "false").await;
        let id = make_id();
        let (mock, _runs) = MockEnvImpl::new();
        let env = Environment::new(mock.with_run_status(false));
        let cmd = Command::new(&log, &id, &env);
        let err = cmd.send("/s").await.unwrap_err();
        assert!(
            matches!(err, EnvironmentError::Run),
            "expected Run error, got {err:?}"
        );
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn send_propagates_expand_error() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "expand-err").await;
        let id = make_id();
        let (mock, _runs) = MockEnvImpl::new();
        let env = Environment::new(mock.with_expand_fail());
        let cmd = Command::new(&log, &id, &env);
        let err = cmd.send("/s").await.unwrap_err();
        assert!(
            matches!(err, EnvironmentError::Implementation { .. }),
            "expected Implementation error, got {err:?}"
        );
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn send_substitutes_path_template() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "send-tmpl").await;
        let id = make_id();
        let (mock, runs) = MockEnvImpl::new();
        let env = Environment::new(mock.with_prefix(PathBuf::from("/sandbox")));
        let mut cmd = Command::new(&log, &id, &env);
        cmd.set("d", "/out").unwrap();
        cmd.send("{{d}}").await.expect("send ok");

        let log = runs.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].0, PathBuf::from("/sandbox/out"));
    }
}
