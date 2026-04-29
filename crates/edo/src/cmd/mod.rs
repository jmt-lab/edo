mod checkout;
mod list;
mod prune;
mod run;
mod update;
mod util;

use std::collections::{BTreeMap, HashMap};

pub use checkout::*;
use edo_core::context::Node;
use edo_core::context::{Addr, Context, LogVerbosity};
use edo_core_plugin::register_core;
pub use list::*;
pub use prune::*;
pub use run::*;
pub use update::*;

use crate::Args;
use crate::Result;

pub async fn create_context(
    args: &Args,
    variables: HashMap<String, String>,
    locked: bool,
) -> Result<Context> {
    let verbosity = if args.trace {
        LogVerbosity::Trace
    } else if args.debug {
        LogVerbosity::Debug
    } else {
        LogVerbosity::Info
    };
    let ctx = Context::init(
        args.storage.clone(),
        args.config.clone(),
        variables,
        verbosity,
    )
    .await?;
    // Register all core component handlers
    register_core(&ctx);
    // Register a local farm in the project directory
    ctx.add_farm(
        &Addr::parse("//default").unwrap(),
        &Node::new_definition("environment", "local", "default", BTreeMap::new()),
    )
    .await?;
    // Now load the current project
    ctx.load_project(locked).await?;
    Ok(ctx)
}
