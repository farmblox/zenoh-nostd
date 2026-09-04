use super::{FixedCapacitySample, Sample, SampleKind};
use zenoh_proto::{CollectionError, exts::EntityGlobalId, fields::ZenohIdProto, keyexpr};

fn responder() -> EntityGlobalId {
    EntityGlobalId {
        zid: ZenohIdProto::try_from(&[7_u8][..]).unwrap(),
        eid: 42,
    }
}

#[test]
fn fixed_capacity_samples_preserve_opaque_attachments() {
    let key = keyexpr::new("test/current/value").unwrap();
    let borrowed = Sample::from_wire(
        key,
        b"value",
        Some(b"causal-stamp"),
        Some(responder()),
        SampleKind::Put,
    );
    let owned = FixedCapacitySample::<64, 16, 32>::try_from(&borrowed).unwrap();

    assert_eq!(owned.keyexpr(), key);
    assert_eq!(owned.payload(), b"value");
    assert_eq!(owned.attachment(), Some(b"causal-stamp".as_slice()));
    assert_eq!(owned.responder(), Some(responder()));
    assert_eq!(owned.kind(), SampleKind::Put);
    assert_eq!(owned.as_ref().attachment(), borrowed.attachment());
}

#[test]
fn fixed_capacity_samples_reject_oversized_attachments() {
    let key = keyexpr::new("test/current/value").unwrap();
    let borrowed = Sample::from_wire(key, b"value", Some(b"too-large"), None, SampleKind::Put);
    assert!(matches!(
        FixedCapacitySample::<64, 16, 4>::try_from(&borrowed),
        Err(CollectionError::CollectionTooSmall)
    ));
}

#[cfg(feature = "alloc")]
#[test]
fn allocated_samples_preserve_opaque_attachments() {
    let key = keyexpr::new("test/current/value").unwrap();
    let borrowed = Sample::from_wire(
        key,
        &[],
        Some(b"causal-stamp"),
        Some(responder()),
        SampleKind::Delete,
    );
    let owned = super::AllocSample::try_from(&borrowed).unwrap();

    assert_eq!(owned.keyexpr(), key);
    assert!(owned.payload().is_empty());
    assert_eq!(owned.attachment(), Some(b"causal-stamp".as_slice()));
    assert_eq!(owned.responder(), Some(responder()));
    assert_eq!(owned.kind(), SampleKind::Delete);
    assert_eq!(owned.as_ref().attachment(), borrowed.attachment());
}
