# AGENTS.md — Edo

A starting point for AI agents navigating this repository. For deeper topic-level docs, see `.agents/summary/index.md`.

## What Edo Is

Rust build tool (workspace, edition 2024, requires Rust ≥ 1.86). Four pluggable abstractions — **Storage, Source, Environment, Transform** — orchestrated by a `Context` + `Scheduler`. Extensibility via an in-process plugin system.

## Repository Map

```
.
├── Cargo.toml              # workspace root; all crate versions pinned here
├── deny.toml               # cargo-deny policy (licenses, advisories, bans)
├── README.md               # user-facing (note: mentions Starlark — see caveats)
├── docs/
│   ├── design.md           # aspirational design doc (predates TOML switch)
│   └── components/         # per-component design docs (also pre-TOML)
├── examples/               # NOT a workspace member (excluded)
│   ├── edo.toml            # umbrella; each subdir has its own edo.toml
│   ├── hello_rust/         # cargo-vendor + offline build example
│   └── hello_oci/          # container farm example (flagged broken in README)
└── crates/
    ├── edo/                # CLI binary (`edo`)
    │   └── src/cmd/        # one file per subcommand
    ├── edo-core/           # engine library
    │   └── src/
    │       ├── context/    # Context, Addr, Config, Node, Schema, Lock, Log, Project
    │       ├── storage/    # Storage + Backend + Artifact/Layer/Id + LocalBackend
    │       ├── source/     # Source, Vendor, resolvo-based resolver
    │       ├── environment/# Environment, Farm, Command (deferred)
    │       ├── transform/  # Transform trait + TransformStatus
    │       ├── scheduler/  # DAG executor (Graph, execute, workers)
    │       ├── plugin/     # plugin host + adapters (impl_/*) + bindings
    │       └── util/       # Reader/Writer/fs/cmd/sync helpers
    ├── edo-plugin-sdk/     # guest-side SDK (Stub defaults)
    ├── edo-wit/            # plugin interface definitions (NOT a Cargo crate)
    └── plugins/
        └── edo-core-plugin/# builtin in-process plugin
            └── src/        # storage/s3, environment/{local,container},
                            # source/{git,local,oci,remote,vendor},
                            # transform/{compose,import,script}, vendor/oci
```

Runtime artefacts (gitignored): `.edo/` (engine working dir + local cache), `edo.lock.json`.

## Key Entry Points

| Topic                  | Where                                                           |
| ---------------------- | --------------------------------------------------------------- |
| CLI `main`             | `crates/edo/src/main.rs`                                        |
| Session bootstrap      | `crates/edo/src/cmd/mod.rs::create_context`                     |
| TOML schema (v1)       | `crates/edo-core/src/context/schema.rs`                         |
| Project loader         | `crates/edo-core/src/context/builder.rs` (`Project`)            |
| DAG scheduler          | `crates/edo-core/src/scheduler/{mod,graph,execute}.rs`          |
| Plugin host            | `crates/edo-core/src/plugin/{mod,host,bindings}.rs` + `impl_/*` |
| Builtin kinds dispatch | `crates/plugins/edo-core-plugin/src/lib.rs::CorePlugin`         |

## Supported Builtin Kinds

Driven by `CorePlugin::supports` in `crates/plugins/edo-core-plugin/src/lib.rs`.

| Component       | Kinds                                       |
| --------------- | ------------------------------------------- |
| Storage backend | `s3`                                        |
| Environment     | `local`, `container`                        |
| Source          | `git`, `local`, `image`, `remote`, `vendor` |
| Transform       | `compose`, `import`, `script`               |
| Vendor          | `image`                                     |

## Addressing

Registry keys use `Addr::parse`. Conventions:

- `//<project>/<name>` — user items from `edo.toml`.
- `//default` — the default `local` farm auto-registered by the CLI.
- `//edo-local-cache`, `//edo-source-cache/<name>`, `//edo-build-cache`, `//edo-output-cache` — storage slots. Only visible in `crates/edo-core/src/storage/mod.rs` comments; remember them when wiring caches.
- `edo` (bare) — the preloaded builtin plugin.

## Repo-Specific Patterns You Will Otherwise Miss

- **`arc_handle` macro**: Every core trait (`Source`, `Environment`, `Farm`, `Transform`, `Backend`, `Plugin`, `Vendor`) is declared with `#[arc_handle]`. You implement `TraitImpl`, wrap with `Trait::new(impl)`, and pass the resulting cheap-to-clone handle. Don't expect bare trait objects.
- **`non_configurable!` macro**: Short-hand for `FromNodeNoContext` impls on types that take no config.
- **`transform_err!` macro** (`edo-core/src/transform/mod.rs`): Use inside `fn transform(...) -> TransformStatus` instead of `?` — it converts `Result::Err` into `TransformStatus::Failed` with logging.
- **Handlebars templating** in `ScriptTransform.commands`. Known variables from examples: `{{install-root}}`, `{{build-root}}`. Read `crates/plugins/edo-core-plugin/src/transform/script.rs` for the full set.
- **`edo-wit` is NOT in the workspace members list**. It has no `Cargo.toml`. Treat it as source-of-truth for the plugin contract.
- **`examples/` is `exclude`d** from the workspace — `cargo build` from the root will not touch it.
- **`default-members = ["crates/edo"]`** — plain `cargo build` / `cargo run` only builds the CLI crate. Use `-p <crate>` or `--workspace` to touch everything.
- **`#[snafu::report]` on `main`** — panics/errors get formatted reports; individual error enums use `#[snafu(transparent)]` heavily to bubble `?` across subsystem boundaries.
- **Storage `fetch_source` vs `fetch`** — prefer `Source::cache(log, storage)` (cache-aware) over direct `fetch` (always re-pulls).
- **`#[macro_use] extern crate tracing;`** at the top of library crates — tracing macros (`info!`, `debug!`, `error!`) are available unqualified inside `edo-core` and `edo-core-plugin`. Match this style when adding new modules.

## Caveats (from `.agents/summary/review_notes.md`)

- `README.md` and `docs/design.md` describe a **Starlark** configuration language. The implementation uses **TOML** (`schema-version = "1"`). Trust `crates/edo-core/src/context/schema.rs` and any `examples/**/edo.toml`, not the README, for config syntax.
- `docs/design.md` references a "Build Engine" type that does not exist; responsibilities live in `Context` + `Scheduler`.
- README "Quick Start", "Configuration Reference", and "Plugin Development" are explicit TODOs.
- `examples/hello_oci` is flagged broken in `examples/README.md`.
- `.gitignore` pattern `**.lock.json` is broader than `edo.lock.json` alone — be deliberate about committing files with that suffix.

## Policy / Config Files

- `deny.toml` — `cargo-deny` config. Run before adding a new crate or git dependency; licenses and ban lists are enforced here.
- No CI workflows, `rust-toolchain.toml`, `Makefile`, or `justfile` are committed.
- `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `LICENSE-APACHE`, `LICENSE-MIT`, `NOTICE` — standard awslabs project files.

## Cross-References

Deeper topic docs (regenerable via the `codebase-summary` SOP):

- `.agents/summary/index.md` — knowledge base entry point & routing table
- `.agents/summary/architecture.md` — how everything fits together
- `.agents/summary/components.md` — per-file reference
- `.agents/summary/interfaces.md` — CLI, TOML schema, traits
- `.agents/summary/data_models.md` — Node, Addr, Artifact, Lock, errors
- `.agents/summary/workflows.md` — run/checkout/update sequences
- `.agents/summary/dependencies.md` — external crates and their roles
- `.agents/summary/review_notes.md` — known doc/code inconsistencies

## Custom Instructions

<!-- This section is maintained by developers and agents during day-to-day work.
     It is NOT auto-generated by codebase-summary and MUST be preserved during refreshes.
     Add project-specific conventions, gotchas, and workflow requirements here. -->
