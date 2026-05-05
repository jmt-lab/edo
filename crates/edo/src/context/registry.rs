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
            kind,
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
            kind,
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
            kind,
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
            kind,
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
            kind,
        })?;
        constructor
            .call(addr.clone(), node.clone(), ctx.clone())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_backend_handler() -> Arc<dyn Handler<Backend>> {
        Arc::new(|_addr: Addr, _node: Node, _ctx: Context| async move {
            unreachable!("not invoked")
        })
    }

    fn dummy_farm_handler() -> Arc<dyn Handler<Farm>> {
        Arc::new(|_addr: Addr, _node: Node, _ctx: Context| async move {
            unreachable!("not invoked")
        })
    }

    fn dummy_source_handler() -> Arc<dyn Handler<Source>> {
        Arc::new(|_addr: Addr, _node: Node, _ctx: Context| async move {
            unreachable!("not invoked")
        })
    }

    fn dummy_transform_handler() -> Arc<dyn Handler<Transform>> {
        Arc::new(|_addr: Addr, _node: Node, _ctx: Context| async move {
            unreachable!("not invoked")
        })
    }

    fn dummy_vendor_handler() -> Arc<dyn Handler<Vendor>> {
        Arc::new(|_addr: Addr, _node: Node, _ctx: Context| async move {
            unreachable!("not invoked")
        })
    }

    #[test]
    fn default_registry_has_empty_maps() {
        let r = Registry::default();
        assert!(r.backends.is_empty());
        assert!(r.farms.is_empty());
        assert!(r.sources.is_empty());
        assert!(r.transforms.is_empty());
        assert!(r.vendors.is_empty());
    }

    #[test]
    fn register_backend_inserts_into_map() {
        let r = Registry::default();
        r.register_backend("kind-backend", dummy_backend_handler());
        assert_eq!(r.backends.len(), 1);
        assert!(r.backends.contains_key("kind-backend"));
    }

    #[test]
    fn register_farm_inserts_into_map() {
        let r = Registry::default();
        r.register_farm("kind-farm", dummy_farm_handler());
        assert_eq!(r.farms.len(), 1);
        assert!(r.farms.contains_key("kind-farm"));
    }

    #[test]
    fn register_source_inserts_into_map() {
        let r = Registry::default();
        r.register_source("kind-source", dummy_source_handler());
        assert_eq!(r.sources.len(), 1);
        assert!(r.sources.contains_key("kind-source"));
    }

    #[test]
    fn register_transform_inserts_into_map() {
        let r = Registry::default();
        r.register_transform("kind-transform", dummy_transform_handler());
        assert_eq!(r.transforms.len(), 1);
        assert!(r.transforms.contains_key("kind-transform"));
    }

    #[test]
    fn register_vendor_inserts_into_map() {
        let r = Registry::default();
        r.register_vendor("kind-vendor", dummy_vendor_handler());
        assert_eq!(r.vendors.len(), 1);
        assert!(r.vendors.contains_key("kind-vendor"));
    }
}
