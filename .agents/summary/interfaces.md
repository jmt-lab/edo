# Interfaces

## CLI Interface

Binary: `edo`. Defined in `crates/edo/src/main.rs`.

```
edo [GLOBAL FLAGS] <SUBCOMMAND> [ARGS...]

Global flags:
  -d, --debug              Enable debug logging (LogVerbosity::Debug)
  -t, --trace              Enable trace logging (LogVerbosity::Trace)
  -c, --config <PATH>      Override edo.toml location
  -s, --storage <PATH>     Override storage / working dir (defaults to `.edo/`)

Subcommands:
  run      <ADDR> [--arg K=V]...         Build a transform
  checkout <ADDR> <OUTPUT_DIR> [--arg K=V]...   Build then extract artifact layers
  prune                                  Prune cached artifacts
  update                                 Refresh edo.lock.json
  list                                   List transforms/addresses
```

`--arg K=V` pairs become the `variables` `HashMap<String, String>` threaded through `Context::init`; transforms retrieve them via `context.get_arg(name)` (see `host.wit::context.get-arg`).

## Project Manifest (`edo.toml`) Interface

Top-level schema dispatch via `schema-version` (currently only `"1"`). Defined in `crates/edo-core/src/context/schema.rs`.

Sections (all TOML tables, keyed by logical name → becomes an address like `//<project>/<name>`):

| Section             | Kind field examples                         | Notes                                            |
| ------------------- | ------------------------------------------- | ------------------------------------------------ |
| `[config]`          | scheduler, log, etc.                        | Arbitrary engine config (`Config::get`).         |
| `[cache.source.*]`  | e.g. `s3`                                   | Registers source caches in `Storage`.            |
| `[cache.build]`     | `s3`                                        | Optional single build cache.                     |
| `[cache.output]`    | `s3`                                        | Optional single output cache.                    |
| `[plugin.<n>]`      | wasm plugin definitions                     | Loaded as `WasmPlugin`.                          |
| `[environment.<n>]` | `local`, `container`                        | Registers a `Farm`.                              |
| `[source.<n>]`      | `local`, `git`, `remote`, `image`, `vendor` | Registers a `Source`.                            |
| `[transform.<n>]`   | `compose`, `import`, `script`               | Registers a `Transform`.                         |
| `[vendor.<n>]`      | `image`                                     | Registers a `Vendor`.                            |
| `[requires.<n>]`    | e.g. `image`                                | Declares a vendored dependency (name + version). |

Templating: `ScriptTransform.commands` are rendered with `handlebars`. Observed variables in examples: `{{install-root}}`, `{{build-root}}`.

## Core Rust Traits (Library Interface)

All four major abstractions are `arc_handle` traits: you implement `TraitImpl`, wrap with `Trait::new(impl)`, and the resulting handle is `Clone + Send + Sync`.

### `Source`

```rust
#[async_trait]
trait Source {
    async fn get_unique_id(&self) -> SourceResult<Id>;
    async fn fetch(&self, log: &Log, storage: &Storage) -> SourceResult<Artifact>;
    async fn stage(&self, log: &Log, storage: &Storage,
                   env: &Environment, path: &Path) -> SourceResult<()>;
}
// plus inherent: Source::cache(log, storage) -> Artifact  (fetch-if-missing)
```

### `Vendor`

Resolve `(name, version)` → `Node` for inclusion in the DAG; retrieve options and transitive dependencies. See `edo-core/src/source/vendor.rs`.

### `Environment`

Full interface in `edo-core/src/environment/mod.rs`. Lifecycle: `setup` → `up` → (`write`/`unpack`/`cmd`/`run`/`read`)\* → `down` → `clean`. `defer_cmd(log, id)` returns a `Command` builder.

### `Farm`

```rust
#[async_trait]
trait Farm {
    async fn setup(&self, log: &Log, storage: &Storage) -> EnvResult<()>;
    async fn create(&self, log: &Log, path: &Path) -> EnvResult<Environment>;
}
```

### `Transform`

```rust
#[async_trait]
trait Transform {
    async fn environment(&self) -> TransformResult<Addr>;
    async fn get_unique_id(&self, ctx: &Handle) -> TransformResult<Id>;
    async fn depends(&self) -> TransformResult<Vec<Addr>>;
    async fn prepare(&self, log: &Log, ctx: &Handle) -> TransformResult<()>;
    async fn stage(&self, log: &Log, ctx: &Handle, env: &Environment) -> TransformResult<()>;
    async fn transform(&self, log: &Log, ctx: &Handle, env: &Environment) -> TransformStatus;
    fn can_shell(&self) -> bool;
    fn shell(&self, env: &Environment) -> TransformResult<()>;
}
```

### `Backend`

See `edo-core/src/storage/backend.rs`. Methods mirror `abi.wit::backend`: `ls`, `has`, `open`, `save`, `del`, `copy`, `prune`, `prune_all`, `read`, `start_layer`, `finish_layer`.

### `Plugin` / `PluginImpl`

```rust
#[async_trait]
trait Plugin {
    async fn fetch(&self, log: &Log, storage: &Storage) -> Result<()>;
    async fn setup(&self, log: &Log, storage: &Storage) -> Result<()>;
    async fn supports(&self, ctx: &Context, component: Component, kind: String) -> Result<bool>;
    async fn create_storage  (&self, addr: &Addr, node: &Node, ctx: &Context) -> Result<Backend>;
    async fn create_farm     (&self, addr: &Addr, node: &Node, ctx: &Context) -> Result<Farm>;
    async fn create_source   (&self, addr: &Addr, node: &Node, ctx: &Context) -> Result<Source>;
    async fn create_transform(&self, addr: &Addr, node: &Node, ctx: &Context) -> Result<Transform>;
    async fn create_vendor   (&self, addr: &Addr, node: &Node, ctx: &Context) -> Result<Vendor>;
}
```

## WebAssembly Plugin Interface (WIT)

Package: `edo:plugin@1.0.0` in `crates/edo-wit/`.

**World** (`edo.wit`):

```wit
world edo {
    use host.{node, error};
    import host;
    export abi;
}
```

**Guest exports** (`abi.wit::abi`): resources `backend`, `environment`, `farm`, `source`, `transform`, `vendor` + five `create-*` factories + a `supports(component, kind) -> bool` predicate.

**Host imports** (`host.wit::host`): resources `reader`, `writer`, `id`, `layer`, `artifact`, `artifact-config`, `storage`, `config`, `log`, `command`, `environment`, `source`, `handle`, `context`, `farm`, `transform`, `node`, `error`; helpers `info` / `warn` / `fatal`; variant `transform-status { success(artifact), retryable(tuple<option<string>, error>), failed(tuple<option<string>, error>) }`; enum `component { storage-backend, environment, source, transform, vendor }`.

Two things cross the boundary as **first-class resources** that are easy to miss:

1. `command` — a host-owned scriptable build command (`chdir`/`pushd`/`popd`/`create-dir`/`copy`/`run`/`send`/…). Obtained from `environment.defer-cmd(log, id)`.
2. `node` — a generic TOML-like data tree with `as-bool`/`as-int`/`as-string`/`as-list`/`as-table`/`get-id`/`get-kind`/`get-name`/`validate-keys`. All plugin factories receive one of these for their section.

## Plugin Authoring (Guest Side)

Depend on `edo-plugin-sdk`, then:

```rust
use edo_plugin_sdk::bindings::exports::edo::plugin::abi;
use edo_plugin_sdk::stub::Stub;

impl abi::GuestSource for MyThing { /* ... */ }
// Use Stub as a default for any Guest* trait you don't need to implement.
```

`wit-bindgen` generates the trait set from `crates/edo-wit/*.wit`.

## Addressing Scheme

`Addr::parse(s: &str)` accepts:

- `//<segment>/<segment>/...` — absolute project/workspace addresses.
- `<bare-name>` — used for built-in items such as the `edo` plugin.

Reserved addresses used by the engine:

- `//default` — the default local `Farm` registered by the CLI.
- `//edo-local-cache`, `//edo-source-cache/<name>`, `//edo-build-cache`, `//edo-output-cache`.
