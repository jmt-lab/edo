//! Configuration traits and user-level config loading.
//!
//! Provides the [`Definable`] and [`DefinableNoContext`] traits that allow
//! plugins and components to declare a configuration key and accept
//! configuration from either the node definition or the user's global
//! `~/.config/edo.toml`. [`NonConfigurable`] is a zero-cost marker for
//! components that need no configuration. [`Config`] loads and queries the
//! user-level configuration file.

use super::{Addr, Context, ContextResult as Result, FromNode, FromNodeNoContext, Node, error};
use async_trait::async_trait;
use home::home_dir;
use snafu::{OptionExt, ResultExt};
use std::{collections::BTreeMap, marker::PhantomData, path::Path};

/// A component that can be configured from a [`Node`] with access to the
/// build [`Context`].
///
/// Implementors declare a [`key`](Definable::key) used to look up
/// configuration in the user's global config file when no inline `config`
/// block is present.
#[async_trait]
pub trait Definable<E, C: FromNode<Error = E> + Send + Default>: FromNode<Error = E> {
    /// Constructs the component from a node, resolving configuration from the
    /// inline `config` block, the global config file, or the default.
    async fn new(addr: &Addr, node: &Node, ctx: &Context) -> std::result::Result<Self, E> {
        let config_key = Self::key();
        let config_node =
            if let Some(cnode) = node.get_table().as_ref().and_then(|x| x.get("config")) {
                Some(C::from_node(addr, cnode, ctx).await?)
            } else if let Some(cnode) = ctx.config().get(config_key) {
                Some(C::from_node(addr, &cnode, ctx).await?)
            } else {
                None
            };
        let mut me = Self::from_node(addr, node, ctx).await?;
        if let Some(config) = config_node.as_ref() {
            me.set_config(config)?;
        } else {
            me.set_config(&C::default())?;
        }
        Ok(me)
    }

    /// Returns the configuration key used to look up settings in the global config.
    fn key() -> &'static str;
    /// Applies the resolved configuration to this component.
    fn set_config(&mut self, config: &C) -> std::result::Result<(), E>;
}

/// Like [`Definable`], but for components that do not need a [`Context`]
/// reference during construction.
#[async_trait]
pub trait DefinableNoContext<E, C: FromNodeNoContext<Error = E> + Send + Default>:
    FromNodeNoContext<Error = E>
{
    /// Constructs the component from a node without a [`Context`], resolving
    /// configuration from the inline block, the global config, or the default.
    async fn new(addr: &Addr, node: &Node, config: &Config) -> std::result::Result<Self, E> {
        let config_key = Self::key();
        let config_node =
            if let Some(cnode) = node.get_table().as_ref().and_then(|x| x.get("config")) {
                Some(C::from_node(addr, cnode, config).await?)
            } else if let Some(cnode) = config.get(config_key) {
                Some(C::from_node(addr, &cnode, config).await?)
            } else {
                None
            };
        let mut me = Self::from_node(addr, node, config).await?;
        if let Some(config) = config_node.as_ref() {
            me.set_config(config)?;
        } else {
            me.set_config(&C::default())?;
        }
        Ok(me)
    }

    /// Returns the configuration key used to look up settings in the global config.
    fn key() -> &'static str;
    /// Applies the resolved configuration to this component.
    fn set_config(&mut self, config: &C) -> std::result::Result<(), E>;
}

/// A zero-size marker type implementing [`FromNode`] and [`FromNodeNoContext`]
/// for components that require no configuration.
pub struct NonConfigurable<E> {
    _data: PhantomData<E>,
}

impl<E> Default for NonConfigurable<E> {
    fn default() -> Self {
        Self { _data: PhantomData }
    }
}

unsafe impl<E> Send for NonConfigurable<E> {}

#[async_trait]
impl<E> FromNode for NonConfigurable<E> {
    type Error = E;

    async fn from_node(
        _addr: &Addr,
        _node: &Node,
        _: &super::Context,
    ) -> std::result::Result<Self, E> {
        Ok(Self { _data: PhantomData })
    }
}

#[async_trait]
impl<E> FromNodeNoContext for NonConfigurable<E> {
    type Error = E;

    async fn from_node(_addr: &Addr, _node: &Node, _: &Config) -> std::result::Result<Self, E> {
        Ok(Self { _data: PhantomData })
    }
}

/// User-level configuration loaded from `~/.config/edo.toml` (or a custom path).
#[derive(Clone)]
pub struct Config {
    configs: BTreeMap<String, Node>,
}

impl Config {
    /// Loads user-level configuration from the given path, or from
    /// `~/.config/edo.toml` if no path is provided.
    pub async fn load<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let path = if let Some(path) = path {
            path.as_ref().to_path_buf()
        } else {
            home_dir()
                .context(error::HomeSnafu)?
                .join(".config/edo.toml")
        };
        if !path.exists() {
            return Ok(Self {
                configs: BTreeMap::new(),
            });
        }
        let bytes = tokio::fs::read(path).await.context(error::IoSnafu)?;
        let configs = toml::from_slice(&bytes).context(error::DeserializeSnafu)?;

        Ok(Self { configs })
    }

    /// Returns the configuration node for the given key, if present.
    pub fn get(&self, name: &str) -> Option<Node> {
        self.configs.get(name).cloned()
    }
}
