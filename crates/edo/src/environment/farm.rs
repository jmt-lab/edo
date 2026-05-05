use super::EnvResult;
use super::Environment;
use crate::context::Log;
use crate::storage::Storage;
use arc_handle::arc_handle;
use async_trait::async_trait;
use std::path::Path;

/// An Environment farm determines how to create new build environments for a transform
/// to run in. Implementations should implement FarmImpl
#[arc_handle]
#[async_trait]
pub trait Farm {
    /// Setup can be used for any one time initializations required for a farm
    async fn setup(&self, log: &Log, storage: &Storage) -> EnvResult<()>;
    /// Create a new environment using this farm
    async fn create(&self, log: &Log, path: &Path) -> EnvResult<Environment>;
}

#[cfg(test)]
mod tests {
    //! Unit tests for the [`Farm`] arc_handle trait plumbing.
    //!
    //! Uses an inline [`CountingFarmImpl`] that records setup/create call
    //! counts via `AtomicUsize` so we can assert (a) `Farm::new` produces a
    //! cheaply-cloneable handle whose clones share state, and (b) both
    //! methods proxy through to the underlying impl — including the error
    //! path.
    //!
    //! A small helper `tmp_storage` mirrors `context/handle.rs::tmp_storage`
    //! to satisfy `Farm::setup`'s `&Storage` parameter without touching the
    //! rest of the context layer.
    use super::*;
    use crate::context::test_support::shared_log_manager;
    use crate::context::{Addr, Config, Log, Node};
    use crate::environment::EnvironmentImpl;
    use crate::environment::error::EnvironmentError;
    use crate::environment::Command;
    use crate::storage::{Backend, Id, LocalBackend, Storage};
    use crate::util::{Reader, Writer};
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::TempDir;

    /// Minimal `EnvironmentImpl` used as the return value of the counting
    /// farm's `create`. `expand` is the only method exercised by the tests.
    struct TinyEnvImpl;

    #[async_trait]
    impl EnvironmentImpl for TinyEnvImpl {
        async fn expand(&self, path: &Path) -> EnvResult<PathBuf> {
            Ok(path.to_path_buf())
        }
        async fn create_dir(&self, _p: &Path) -> EnvResult<()> {
            Ok(())
        }
        async fn set_env(&self, _k: &str, _v: &str) -> EnvResult<()> {
            Ok(())
        }
        async fn get_env(&self, _k: &str) -> Option<String> {
            None
        }
        async fn setup(&self, _log: &Log, _storage: &Storage) -> EnvResult<()> {
            Ok(())
        }
        async fn up(&self, _log: &Log) -> EnvResult<()> {
            Ok(())
        }
        async fn down(&self, _log: &Log) -> EnvResult<()> {
            Ok(())
        }
        async fn clean(&self, _log: &Log) -> EnvResult<()> {
            Ok(())
        }
        async fn write(&self, _p: &Path, _r: Reader) -> EnvResult<()> {
            Ok(())
        }
        async fn unpack(&self, _p: &Path, _r: Reader) -> EnvResult<()> {
            Ok(())
        }
        async fn read(&self, _p: &Path, _w: Writer) -> EnvResult<()> {
            Ok(())
        }
        async fn cmd(
            &self,
            _log: &Log,
            _id: &Id,
            _p: &Path,
            _c: &str,
        ) -> EnvResult<bool> {
            Ok(true)
        }
        async fn run(
            &self,
            _log: &Log,
            _id: &Id,
            _p: &Path,
            _c: &Command,
        ) -> EnvResult<bool> {
            Ok(true)
        }
        fn shell(&self, _p: &Path) -> EnvResult<()> {
            Ok(())
        }
    }

    /// A farm impl that counts invocations and can be configured to fail.
    struct CountingFarmImpl {
        setup_calls: Arc<AtomicUsize>,
        create_calls: Arc<AtomicUsize>,
        setup_fail: Arc<AtomicBool>,
        create_fail: Arc<AtomicBool>,
    }

    impl CountingFarmImpl {
        fn new() -> Self {
            Self {
                setup_calls: Arc::new(AtomicUsize::new(0)),
                create_calls: Arc::new(AtomicUsize::new(0)),
                setup_fail: Arc::new(AtomicBool::new(false)),
                create_fail: Arc::new(AtomicBool::new(false)),
            }
        }
        fn handles(&self) -> CountingHandles {
            CountingHandles {
                setup_calls: self.setup_calls.clone(),
                create_calls: self.create_calls.clone(),
                setup_fail: self.setup_fail.clone(),
                create_fail: self.create_fail.clone(),
            }
        }
    }

    /// External handles kept by the test so it can read counters / flip
    /// error flags independently of the impl once it has been moved into
    /// `Farm::new`.
    struct CountingHandles {
        setup_calls: Arc<AtomicUsize>,
        create_calls: Arc<AtomicUsize>,
        setup_fail: Arc<AtomicBool>,
        create_fail: Arc<AtomicBool>,
    }

    #[async_trait]
    impl FarmImpl for CountingFarmImpl {
        async fn setup(&self, _log: &Log, _storage: &Storage) -> EnvResult<()> {
            self.setup_calls.fetch_add(1, Ordering::SeqCst);
            if self.setup_fail.load(Ordering::SeqCst) {
                return Err(EnvironmentError::Implementation {
                    source: Box::new(std::io::Error::other("setup failed")),
                });
            }
            Ok(())
        }
        async fn create(&self, _log: &Log, _path: &Path) -> EnvResult<Environment> {
            self.create_calls.fetch_add(1, Ordering::SeqCst);
            if self.create_fail.load(Ordering::SeqCst) {
                return Err(EnvironmentError::Implementation {
                    source: Box::new(std::io::Error::other("create failed")),
                });
            }
            Ok(Environment::new(TinyEnvImpl))
        }
    }

    /// Build a minimal `Storage` backed by a temporary local directory.
    /// Mirrors `context/handle.rs::tests::tmp_storage`.
    async fn tmp_storage(dir: &std::path::Path) -> Storage {
        let addr = Addr::parse("//edo-test-cache").unwrap();
        let mut table = BTreeMap::new();
        table.insert(
            "path".to_string(),
            Node::new_string(dir.to_string_lossy().to_string()),
        );
        let node = Node::new_definition("storage", "local", "test", table);
        let config = Config::load::<&std::path::Path>(None).await.unwrap();
        let local = <LocalBackend as crate::context::DefinableNoContext<
            crate::storage::StorageError,
            crate::context::NonConfigurable<crate::storage::StorageError>,
        >>::new(&addr, &node, &config)
        .await
        .unwrap();
        Storage::init(&Backend::new(local)).await.unwrap()
    }

    async fn make_log(dir: &TempDir, name: &str) -> Log {
        let mgr = shared_log_manager().await;
        let path = dir.path().join(format!("{name}.log"));
        Log::new(&mgr, &path).expect("Log::new")
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn farm_new_wraps_impl_and_is_cheaply_cloneable() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "farm-clone").await;
        let storage = tmp_storage(dir.path()).await;

        let impl_ = CountingFarmImpl::new();
        let handles = impl_.handles();
        let farm = Farm::new(impl_);

        let a = farm.clone();
        let b = farm.clone();
        let c = farm.clone();

        a.setup(&log, &storage).await.unwrap();
        b.setup(&log, &storage).await.unwrap();
        c.setup(&log, &storage).await.unwrap();

        // All three clones share a single counter, proving that
        // `Farm::new` produces a handle wrapping a single `Arc<impl>`.
        assert_eq!(handles.setup_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn farm_setup_invokes_impl() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "farm-setup").await;
        let storage = tmp_storage(dir.path()).await;

        let impl_ = CountingFarmImpl::new();
        let handles = impl_.handles();
        let farm = Farm::new(impl_);

        farm.setup(&log, &storage).await.expect("setup ok");
        assert_eq!(handles.setup_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn farm_create_returns_environment() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "farm-create").await;

        let impl_ = CountingFarmImpl::new();
        let handles = impl_.handles();
        let farm = Farm::new(impl_);

        let env = farm.create(&log, dir.path()).await.expect("create ok");
        assert_eq!(handles.create_calls.load(Ordering::SeqCst), 1);

        // The returned environment is usable — reaching `expand` proves the
        // underlying `EnvironmentImpl` is wired up end-to-end.
        let expanded = env.expand(Path::new("/x")).await.expect("expand ok");
        assert_eq!(expanded, PathBuf::from("/x"));
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn farm_create_propagates_impl_error() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "farm-create-err").await;

        let impl_ = CountingFarmImpl::new();
        let handles = impl_.handles();
        handles.create_fail.store(true, Ordering::SeqCst);
        let farm = Farm::new(impl_);

        let result = farm.create(&log, dir.path()).await;
        match result {
            Err(EnvironmentError::Implementation { .. }) => {}
            Err(other) => panic!("expected Implementation error, got {other:?}"),
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[tokio::test]
    #[serial_test::serial(log_manager)]
    async fn farm_setup_propagates_impl_error() {
        let dir = TempDir::new().unwrap();
        let log = make_log(&dir, "farm-setup-err").await;
        let storage = tmp_storage(dir.path()).await;

        let impl_ = CountingFarmImpl::new();
        let handles = impl_.handles();
        handles.setup_fail.store(true, Ordering::SeqCst);
        let farm = Farm::new(impl_);

        let err = farm.setup(&log, &storage).await.unwrap_err();
        assert!(
            matches!(err, EnvironmentError::Implementation { .. }),
            "expected Implementation error, got {err:?}"
        );
    }
}
