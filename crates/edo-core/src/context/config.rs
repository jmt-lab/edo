use super::{error, Addr, Context, ContextResult as Result, FromNode, FromNodeNoContext, Node};
use async_trait::async_trait;
use home::home_dir;
use snafu::{OptionExt, ResultExt};
use std::{collections::BTreeMap, marker::PhantomData, path::Path};

#[async_trait]
pub trait Definable<E, C: FromNode<Error = E> + Send + Default>: FromNode<Error = E> {
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

    fn key() -> &'static str;
    fn set_config(&mut self, config: &C) -> std::result::Result<(), E>;
}

#[async_trait]
pub trait DefinableNoContext<E, C: FromNodeNoContext<Error = E> + Send + Default>:
    FromNodeNoContext<Error = E>
{
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

    fn key() -> &'static str;
    fn set_config(&mut self, config: &C) -> std::result::Result<(), E>;
}

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

#[derive(Clone)]
pub struct Config {
    configs: BTreeMap<String, Node>,
}

impl Config {
    pub async fn load<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let path = if let Some(path) = path {
            path.as_ref().to_path_buf()
        } else {
            home_dir().context(error::HomeSnafu)?.join(".config/edo.toml")
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

    pub fn get(&self, name: &str) -> Option<Node> {
        self.configs.get(name).cloned()
    }
}
