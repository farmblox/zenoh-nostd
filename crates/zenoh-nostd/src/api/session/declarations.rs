//! Local declarations that must survive a transport reconnect.
//!
//! A Zenoh session is longer-lived than any one transport. Subscribers,
//! queryables, and interests belong to that session, so a replacement
//! transport must announce the same declarations before normal traffic
//! resumes. This table is the constrained equivalent of the declaration state
//! retained by the mainline Zenoh runtime.

use heapless::FnvIndexMap;
use zenoh_proto::{
    CollectionError, keyexpr,
    msgs::{InterestMode, InterestOptions},
};
#[cfg(feature = "alloc")]
use zenoh_proto::{
    exts::QoS,
    fields::{Reliability, WireExpr},
    msgs::*,
};

/// One declaration owned by the local session.
#[derive(Clone, Copy)]
pub enum Declaration {
    Subscriber {
        id: u32,
        key: &'static keyexpr,
    },
    Queryable {
        id: u32,
        key: &'static keyexpr,
    },
    Interest {
        id: u32,
        key: &'static keyexpr,
        mode: InterestMode,
        options: InterestOptions,
    },
}

impl Declaration {
    pub(crate) const fn id(self) -> u32 {
        match self {
            Self::Subscriber { id, .. }
            | Self::Queryable { id, .. }
            | Self::Interest { id, .. } => id,
        }
    }

    #[cfg(feature = "alloc")]
    pub(crate) fn message(self) -> NetworkMessage<'static> {
        let body = match self {
            Self::Subscriber { id, key } => NetworkBody::Declare(Declare {
                qos: QoS::declare(),
                body: DeclareBody::DeclareSubscriber(DeclareSubscriber {
                    id,
                    wire_expr: WireExpr::from(key),
                }),
                ..Default::default()
            }),
            Self::Queryable { id, key } => NetworkBody::Declare(Declare {
                qos: QoS::declare(),
                body: DeclareBody::DeclareQueryable(DeclareQueryable {
                    id,
                    wire_expr: WireExpr::from(key),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            Self::Interest {
                id,
                key,
                mode,
                options,
            } => NetworkBody::Interest(Interest {
                id,
                mode,
                qos: QoS::declare(),
                inner: InterestInner {
                    options: options.options,
                    wire_expr: Some(WireExpr::from(key)),
                },
                ..Default::default()
            }),
        };
        NetworkMessage {
            reliability: Reliability::default(),
            qos: QoS::declare(),
            body,
        }
    }
}

/// Storage policy for local session declarations.
pub trait ZDeclarations {
    fn empty() -> Self;
    fn insert(&mut self, declaration: Declaration) -> Result<(), CollectionError>;
    fn remove(&mut self, id: u32);
    fn iter(&self) -> impl Iterator<Item = Declaration>;
}

/// A bounded declaration table for allocator-free targets.
pub struct FixedCapacityDeclarations<const CAPACITY: usize> {
    declarations: FnvIndexMap<u32, Declaration, CAPACITY>,
}

impl<const CAPACITY: usize> ZDeclarations for FixedCapacityDeclarations<CAPACITY> {
    fn empty() -> Self {
        Self {
            declarations: FnvIndexMap::new(),
        }
    }

    fn insert(&mut self, declaration: Declaration) -> Result<(), CollectionError> {
        if self.declarations.contains_key(&declaration.id()) {
            return Err(CollectionError::KeyAlreadyExists);
        }
        self.declarations
            .insert(declaration.id(), declaration)
            .map_err(|_| CollectionError::CollectionIsFull)
            .map(|_| ())
    }

    fn remove(&mut self, id: u32) {
        self.declarations.remove(&id);
    }

    fn iter(&self) -> impl Iterator<Item = Declaration> {
        self.declarations.values().copied()
    }
}

/// A declaration table for allocator-backed no-std and browser sessions.
#[cfg(feature = "alloc")]
pub struct AllocDeclarations {
    declarations: alloc::collections::BTreeMap<u32, Declaration>,
}

#[cfg(feature = "alloc")]
impl ZDeclarations for AllocDeclarations {
    fn empty() -> Self {
        Self {
            declarations: alloc::collections::BTreeMap::new(),
        }
    }

    fn insert(&mut self, declaration: Declaration) -> Result<(), CollectionError> {
        if self.declarations.contains_key(&declaration.id()) {
            return Err(CollectionError::KeyAlreadyExists);
        }
        self.declarations.insert(declaration.id(), declaration);
        Ok(())
    }

    fn remove(&mut self, id: u32) {
        self.declarations.remove(&id);
    }

    fn iter(&self) -> impl Iterator<Item = Declaration> {
        self.declarations.values().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriber(id: u32, key: &'static str) -> Declaration {
        Declaration::Subscriber {
            id,
            key: keyexpr::new(key).unwrap(),
        }
    }

    #[test]
    fn bounded_declarations_retain_exact_ids_until_undeclared() {
        let mut declarations = FixedCapacityDeclarations::<2>::empty();
        declarations.insert(subscriber(7, "demo/a")).unwrap();
        declarations.insert(subscriber(8, "demo/b")).unwrap();
        assert_eq!(
            declarations
                .iter()
                .map(Declaration::id)
                .collect::<heapless::Vec<_, 2>>(),
            heapless::Vec::<u32, 2>::from_slice(&[7, 8]).unwrap()
        );

        declarations.remove(7);
        assert_eq!(declarations.iter().next().map(Declaration::id), Some(8));
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn duplicate_alloc_id_is_refused_without_replacing_the_declaration() {
        let mut declarations = AllocDeclarations::empty();
        declarations.insert(subscriber(7, "demo/original")).unwrap();
        assert!(
            declarations
                .insert(subscriber(7, "demo/replacement"))
                .is_err()
        );

        let Declaration::Subscriber { key, .. } = declarations.iter().next().unwrap() else {
            panic!("expected subscriber declaration");
        };
        assert_eq!(key.as_str(), "demo/original");
    }

    #[cfg(feature = "alloc")]
    #[test]
    fn replay_message_preserves_declaration_kind_and_id() {
        let message = subscriber(19, "demo/replay").message();
        let NetworkBody::Declare(Declare {
            body: DeclareBody::DeclareSubscriber(declaration),
            ..
        }) = message.body
        else {
            panic!("expected subscriber declaration message");
        };
        assert_eq!(declaration.id, 19);
        assert_eq!(declaration.wire_expr.suffix, "demo/replay");
    }
}
