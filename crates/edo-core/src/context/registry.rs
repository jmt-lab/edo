use crate::{
    context::{Addr, Context, Node, error},
    environment::Farm,
    source::{Source, Vendor},
    storage::Backend,
    transform::Transform,
};
use dashmap::DashMap;
use futures::future::BoxFuture;
use snafu::OptionExt;
use std::sync::Arc;

use super::ContextResult;

pub trait Handler<T>: Send + Sync {
    fn call(
        &self,
        addr: Addr,
        node: Node,
        context: Context,
    ) -> BoxFuture<'static, ContextResult<T>>;
}

impl<T, E, F> Handler<T> for E
where
    E: Fn(Addr, Node, Context) -> F + Send + Sync,
    F: Future<Output = ContextResult<T>> + 'static + Send,
{
    fn call(
        &self,
        addr: Addr,
        node: Node,
        context: Context,
    ) -> BoxFuture<'static, ContextResult<T>> {
        Box::pin(self(addr, node, context))
    }
}

#[derive(Default, Clone)]
pub struct Registry {
    pub backends: DashMap<String, Arc<dyn Handler<Backend>>>,
    pub farms: DashMap<String, Arc<dyn Handler<Farm>>>,
    pub sources: DashMap<String, Arc<dyn Handler<Source>>>,
    pub transforms: DashMap<String, Arc<dyn Handler<Transform>>>,
    pub vendors: DashMap<String, Arc<dyn Handler<Vendor>>>,
}

impl Registry {
    pub fn register_backend(&self, name: &str, handler: Arc<dyn Handler<Backend>>) {
        self.backends.insert(name.to_string(), handler);
    }

    pub async fn backend(&self, addr: &Addr, node: &Node, ctx: &Context) -> ContextResult<Backend> {
        let kind = node.get_kind().context(error::FieldSnafu {
            field: "kind",
            type_: "string",
        })?;
        let constructor = self.backends.get(&kind).context(error::NoProviderSnafu {
            component: "backend",
            kind: kind,
        })?;
        constructor
            .call(addr.clone(), node.clone(), ctx.clone())
            .await
    }

    pub fn register_farm(&self, name: &str, handler: Arc<dyn Handler<Farm>>) {
        self.farms.insert(name.to_string(), handler);
    }

    pub async fn farm(&self, addr: &Addr, node: &Node, ctx: &Context) -> ContextResult<Farm> {
        let kind = node.get_kind().context(error::FieldSnafu {
            field: "kind",
            type_: "string",
        })?;
        let constructor = self.farms.get(&kind).context(error::NoProviderSnafu {
            component: "environment",
            kind: kind,
        })?;
        constructor
            .call(addr.clone(), node.clone(), ctx.clone())
            .await
    }

    pub fn register_source(&self, name: &str, handler: Arc<dyn Handler<Source>>) {
        self.sources.insert(name.to_string(), handler);
    }

    pub async fn source(&self, addr: &Addr, node: &Node, ctx: &Context) -> ContextResult<Source> {
        let kind = node.get_kind().context(error::FieldSnafu {
            field: "kind",
            type_: "string",
        })?;
        let constructor = self.sources.get(&kind).context(error::NoProviderSnafu {
            component: "source",
            kind: kind,
        })?;
        constructor
            .call(addr.clone(), node.clone(), ctx.clone())
            .await
    }

    pub fn register_transform(&self, name: &str, handler: Arc<dyn Handler<Transform>>) {
        self.transforms.insert(name.to_string(), handler);
    }

    pub async fn transform(
        &self,
        addr: &Addr,
        node: &Node,
        ctx: &Context,
    ) -> ContextResult<Transform> {
        let kind = node.get_kind().context(error::FieldSnafu {
            field: "kind",
            type_: "string",
        })?;
        let constructor = self.transforms.get(&kind).context(error::NoProviderSnafu {
            component: "transform",
            kind: kind,
        })?;
        constructor
            .call(addr.clone(), node.clone(), ctx.clone())
            .await
    }

    pub fn register_vendor(&self, name: &str, handler: Arc<dyn Handler<Vendor>>) {
        self.vendors.insert(name.to_string(), handler);
    }

    pub async fn vendor(&self, addr: &Addr, node: &Node, ctx: &Context) -> ContextResult<Vendor> {
        let kind = node.get_kind().context(error::FieldSnafu {
            field: "kind",
            type_: "string",
        })?;
        let constructor = self.vendors.get(&kind).context(error::NoProviderSnafu {
            component: "vendor",
            kind: kind,
        })?;
        constructor
            .call(addr.clone(), node.clone(), ctx.clone())
            .await
    }
}
