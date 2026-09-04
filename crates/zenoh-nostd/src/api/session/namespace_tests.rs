//! Namespace behavior pinned to the mainline Zenoh session boundary.

use super::keyexprs::KeyExprTable;
use super::{project_namespace, remove_namespace};
use zenoh_proto::{fields::WireExpr, keyexpr};

#[test]
fn egress_prefixes_every_application_expression() {
    let namespace =
        <&zenoh_proto::nonwild_keyexpr>::try_from(keyexpr::new("example/local/node-a").unwrap())
            .unwrap();
    let logical = keyexpr::new("tenant/a/log/append").unwrap();
    let mut storage = heapless::String::new();

    let wire = project_namespace(Some(namespace), logical, &mut storage).unwrap();

    assert_eq!(wire.suffix, "example/local/node-a/tenant/a/log/append");
}

#[test]
fn ingress_exposes_only_the_relative_tail() {
    let namespace =
        <&zenoh_proto::nonwild_keyexpr>::try_from(keyexpr::new("example/local/node-a").unwrap())
            .unwrap();

    assert_eq!(
        remove_namespace(Some(namespace), "example/local/node-a/tenant/a/log"),
        Some("tenant/a/log")
    );
    assert_eq!(
        remove_namespace(Some(namespace), "example/local/node-b/tenant/a/log"),
        None,
        "another machine's namespace must be rejected rather than exposed"
    );
    assert_eq!(remove_namespace(Some(namespace), "tenant/a/log"), None);
}

#[test]
fn an_unnamespaced_session_preserves_the_original_key() {
    let logical = keyexpr::new("public/tenant/a/runtime").unwrap();
    let mut storage = heapless::String::new();

    let wire = project_namespace(None, logical, &mut storage).unwrap();

    assert_eq!(wire.suffix, logical.as_str());
    assert_eq!(
        remove_namespace(None, logical.as_str()),
        Some(logical.as_str())
    );
}

#[test]
fn a_mapped_namespace_prefix_resolves_before_ingress_filtering() {
    let namespace =
        <&zenoh_proto::nonwild_keyexpr>::try_from(keyexpr::new("example/local/node-a").unwrap())
            .unwrap();
    let mut mappings = KeyExprTable::new();
    assert!(mappings.declare(7, "example"));

    let wire = WireExpr {
        scope: 7,
        mapping: Default::default(),
        suffix: "/local/node-a/tenant/a/log",
    };
    let mut resolved = heapless::String::new();
    let transport_key = mappings.resolve(&wire, &mut resolved).unwrap();

    assert_eq!(
        remove_namespace(Some(namespace), transport_key),
        Some("tenant/a/log")
    );
}

#[test]
fn ingress_accepts_every_key_that_egress_can_emit() {
    let namespace =
        <&zenoh_proto::nonwild_keyexpr>::try_from(keyexpr::new("example/local/node-a").unwrap())
            .unwrap();
    let mut logical_text = heapless::String::<300>::new();
    logical_text.push_str("tenant/").unwrap();
    for _ in 0..260 {
        logical_text.push('a').unwrap();
    }
    let logical = keyexpr::new(logical_text.as_str()).unwrap();
    let mut projected = heapless::String::new();
    let wire = project_namespace(Some(namespace), logical, &mut projected).unwrap();
    assert!(wire.suffix.len() > super::keyexprs::MAX_MAPPED_KEYEXPR);

    let mappings = KeyExprTable::new();
    let mut resolved = heapless::String::new();
    let transport_key = mappings.resolve(&wire, &mut resolved).unwrap();

    assert_eq!(
        remove_namespace(Some(namespace), transport_key),
        Some(logical.as_str())
    );
}
