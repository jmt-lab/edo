# Components

## Crate-Level Components

```mermaid
classDiagram
    class edo {
        +main()
        +Args (clap)
        +cmd::{Checkout,Run,Prune,Update,List}
        +create_context()
    }
    class edo_core {
        +Context
        +Storage, Backend
        +Source, Vendor
        +Environment, Farm
        +Transform
        +Plugin
        +Scheduler
    }
    class edo_core_plugin {
        +core_plugin()
        +CorePlugin : PluginImpl
        +S3Backend
        +LocalFarm, ContainerFarm
        +GitSource, LocalSource, ImageSource, RemoteSource, VendorSource
        +ComposeTransform, ImportTransform, ScriptTransform
        +ImageVendor
    }
    class edo_plugin_sdk {
        +bindings
        +Stub (default impls)
        +error
    }
    edo --> edo_core
    edo --> edo_core_plugin
    edo_core_plugin --> edo_core
    edo_plugin_sdk ..> edo_core : plugin contract
```

## `crates/edo` — CLI

Binary built as `edo`. Files under `crates/edo/src/cmd/`:

| File          | Subcommand | Purpose                                                                         |
| ------------- | ---------- | ------------------------------------------------------------------------------- |
| `checkout.rs` | `checkout` | Build target then extract its artifact layers into a local dir.                 |
| `run.rs`      | `run`      | Build a transform (`ctx.run(&addr)`).                                           |
| `prune.rs`    | `prune`    | Prune cached artifacts from storage.                                            |
| `list.rs`     | `list`     | List known transforms/addresses.                                                |
| `update.rs`   | `update`   | Refresh the dependency lock file (`edo.lock.json`).                             |
| `util.rs`     | —          | `parse_key_val` helper for `--arg KEY=VALUE`.                                   |
| `mod.rs`      | —          | `create_context()` shared bootstrap (registers core plugin + `//default` farm). |

Top-level flags (`Args`): `--debug`, `--trace`, `-c/--config <path>`, `-s/--storage <path>`.

## `crates/edo-core` — Engine

### `context/`

| File         | Key type / role                                                     |
| ------------ | ------------------------------------------------------------------- |
| `mod.rs`     | `Context` (the shared session handle, `Clone` cheap).               |
| `address.rs` | `Addr`, `Addressable`, `Addr::parse`.                               |
| `builder.rs` | `Project` loader, dependency resolution, `non_configurable!` macro. |
| `config.rs`  | `Config`, `Definable`, `DefinableNoContext`, `NonConfigurable`.     |
| `handle.rs`  | `Handle` — read-only view passed to transforms.                     |
| `lock.rs`    | `Lock` (serialized to `edo.lock.json`).                             |
| `log.rs`     | `Log` — per-task log file handle.                                   |
| `logmgr.rs`  | `LogManager`, `LogVerbosity` (Trace/Debug/Info).                    |
| `node.rs`    | `Node`, `Data`, `Component` enum, `FromNode`, `FromNodeNoContext`.  |
| `schema.rs`  | `Schema::V1` / `SchemaV1` / `Cache` — TOML-to-`Node` conversion.    |
| `error.rs`   | `ContextError`.                                                     |

### `storage/`

| File          | Key type / role                                                                |
| ------------- | ------------------------------------------------------------------------------ |
| `mod.rs`      | `Storage` (composite of local + source caches + optional build/output caches). |
| `backend.rs`  | `Backend` trait (arc_handle).                                                  |
| `local.rs`    | `LocalBackend` — default on-disk backend.                                      |
| `artifact.rs` | `Artifact`, `ArtifactConfig`, `Layer`, `MediaType`, `Compression`.             |
| `id.rs`       | `Id` — content-addressed artifact identifier.                                  |
| `catalog.rs`  | Tracks provides/requires across artifacts.                                     |
| `error.rs`    | `StorageError`, `StorageResult`.                                               |

### `source/`

| File          | Key type / role                                                      |
| ------------- | -------------------------------------------------------------------- |
| `mod.rs`      | `Source` trait + `Source::cache` convenience wrapper around `fetch`. |
| `vendor.rs`   | `Vendor` trait (resolve name+version → `Node`).                      |
| `require.rs`  | `Require` — requirement descriptors.                                 |
| `resolver.rs` | `resolvo`-backed dependency solver.                                  |
| `version.rs`  | Version parsing / matching.                                          |

### `environment/`

| File         | Key type / role                                                                                |
| ------------ | ---------------------------------------------------------------------------------------------- |
| `mod.rs`     | `Environment` trait (expand/create_dir/set_env/up/down/clean/cmd/run/shell/write/unpack/read). |
| `farm.rs`    | `Farm` trait + `create(log, path) -> Environment`.                                             |
| `command.rs` | `Command` — deferred, scriptable build command builder (cd/pushd/popd/copy/run/…).             |
| `error.rs`   | `EnvironmentError`.                                                                            |

### `transform/mod.rs`

- `Transform` trait (see architecture.md for methods).
- `TransformStatus { Success(Artifact), Retryable(Option<PathBuf>, Error), Failed(Option<PathBuf>, Error) }`.
- `transform_err!` macro — maps `Result` into `TransformStatus::Failed` with logging.

### `scheduler/`

| File         | Role                                      |
| ------------ | ----------------------------------------- |
| `mod.rs`     | Public `Scheduler` with `run(ctx, addr)`. |
| `graph.rs`   | `Graph` — DAG add/fetch/run.              |
| `node.rs`    | Per-node execution state.                 |
| `execute.rs` | Worker pool execution.                    |
| `error.rs`   | `SchedulerError`.                         |

Worker count: `[scheduler] workers = <int>` in config, default 8.

### `plugin/`

| File                                                                             | Role                                                                                                                                                          |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mod.rs`                                                                         | `Plugin` (`arc_handle` trait), in-process `PluginImpl` trait.                                                                                   |
| `bindings.rs`                                                                    | Plugin bindings.                                                                                                                                              |
| `host.rs`                                                                        | Host-side resource implementations (storage/log/command/node/...).                                                                                            |
| `impl_/`                                                                         | Adapters that make guest resources look like native traits: `backend.rs`, `environment.rs`, `farm.rs`, `handle.rs`, `source.rs`, `transform.rs`, `vendor.rs`. |
| `environment.rs`, `source.rs`, `storage.rs`, `transform.rs`, `log.rs`, `node.rs` | Host-facing wrappers exposed across the plugin boundary.                                                                                                      |
| `error.rs`                                                                       | `PluginError`, `GuestError`.                                                                                                                                  |

### `util/`

Async `Reader` / `Writer`, `fs` helpers, `command` helpers, `sync` helpers (used by both core and plugins).

## `crates/plugins/edo-core-plugin` — Builtin Plugin

In-process `PluginImpl`. Dispatches in `CorePlugin::supports` / `create_*` by matching `node.get_kind()`.

| Sub-module                 | Provides                                                                                            |
| -------------------------- | --------------------------------------------------------------------------------------------------- |
| `storage/s3`               | `S3Backend` — OCI-layer-aware S3-backed cache.                                                      |
| `environment/local.rs`     | `LocalFarm` — runs commands on the host.                                                            |
| `environment/container.rs` | `ContainerFarm` — runs commands in Docker / Podman / Finch containers.                              |
| `source/local.rs`          | `LocalSource` — files from the project tree (supports `is_archive`).                                |
| `source/git.rs`            | `GitSource` — clone/checkout.                                                                       |
| `source/remote.rs`         | `RemoteSource` — fetch a URL.                                                                       |
| `source/oci.rs`            | `ImageSource` — OCI image layer.                                                                    |
| `source/vendor.rs`         | `VendorSource` — resolve via a registered `Vendor`.                                                 |
| `vendor/oci.rs`            | `ImageVendor` — OCI registry (e.g. `public.ecr.aws/...`).                                           |
| `transform/script.rs`      | `ScriptTransform` — run shell commands (Handlebars-templated, `{{install-root}}`/`{{build-root}}`). |
| `transform/import.rs`      | `ImportTransform` — import sources into an artifact.                                                |
| `transform/compose.rs`     | `ComposeTransform` — compose artifacts from other transforms.                                       |

## `crates/edo-plugin-sdk` — Guest SDK

For authoring third-party plugins. Re-exports `bindings` and supplies `stub::Stub` that implements every `Guest*` trait with `NotImplemented` errors so you can `impl GuestSource for MyThing` and let the rest default.

## `crates/edo-wit` — Plugin Interface Definitions

Three files: `edo.wit`, `host.wit`, `abi.wit`. Consumed by both `edo-core` (host) and `edo-plugin-sdk` (guest).

## Examples (`examples/`)

`examples/edo.toml` is an umbrella workspace with `schema-version = "1"` only. Individual examples:

- `hello_rust/edo.toml` — demonstrates `source.kind = "local"`, `transform.kind = "import"`, and chained `transform.kind = "script"` with `cargo vendor` + offline build.
- `hello_oci/edo.toml` — demonstrates `[vendor]`, `[requires]`, `[environment]` with `kind = "container"`, and a gcc-in-container script build. The example README flags this as currently broken.
