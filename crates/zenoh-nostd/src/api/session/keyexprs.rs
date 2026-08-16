//! The key-expression mapping table.
//!
//! A `WireExpr` is not always a string. Zenoh lets a peer declare a long key
//! once — `DeclareKeyExpr { id, wire_expr }` — and afterwards reference it by
//! that numeric `scope`, carrying only the part that differs in `suffix`:
//!
//! ```text
//!   DeclareKeyExpr { id: 17, wire_expr: "fieldblox/org/…/block/…" }
//!   Declare(DeclareToken { wire_expr: { scope: 17, suffix: "" } })
//! ```
//!
//! Without a table mapping 17 back to its string, that second message is
//! unreadable: `suffix` alone is empty, and parsing it fails with "empty chunk
//! in expression". This is what an interest's `KEYEXPRS` flag is *for* — you
//! ask for the key-expression declarations precisely so the declarations that
//! reference them can be resolved.
//!
//! ## Bounded, and honest when it fills
//!
//! Fixed capacity, like every other table here — a peer that has no allocator
//! cannot let a router decide how much memory it uses. When it is full, a new
//! mapping is refused rather than evicting an old one: eviction would make an
//! id that is still in use silently unresolvable later, which reads as "that
//! key does not exist" rather than as the resource exhaustion it is.

use heapless::{FnvIndexMap, String};
use zenoh_proto::{fields::*, *};

/// How many mappings one session remembers.
///
/// A power of two because `FnvIndexMap` requires it. Sixteen covers the shapes
/// a client actually declares interest in — a handful of wildcards, each
/// mapped once — without reserving space a microcontroller does not have.
pub const MAX_KEYEXPR_MAPPINGS: usize = 16;

/// The longest key expression a mapping can hold.
pub const MAX_MAPPED_KEYEXPR: usize = 256;

/// Numeric key-expression ids to the strings they stand for.
#[derive(Default)]
pub struct KeyExprTable {
    map: FnvIndexMap<u16, String<MAX_MAPPED_KEYEXPR>, MAX_KEYEXPR_MAPPINGS>,
}

impl KeyExprTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Remember `id -> ke`, from an inbound `DeclareKeyExpr`.
    ///
    /// Returns `false` when the table is full, the expression does not fit, or
    /// the id is zero — so the caller can say so once rather than discovering
    /// it later as an unresolvable reference.
    ///
    /// **Zero is not a usable id.** `scope == 0` is how a `WireExpr` says "I am
    /// not a reference, I am the literal suffix", so a mapping stored at 0
    /// could never be looked up. Refusing it is better than accepting a
    /// mapping that is unreachable by construction.
    pub fn declare(&mut self, id: u16, ke: &str) -> bool {
        if id == 0 {
            return false;
        }
        let Ok(owned) = String::try_from(ke) else {
            return false;
        };
        self.map.insert(id, owned).is_ok()
    }

    /// Forget `id`, from an inbound `UndeclareKeyExpr`.
    pub fn undeclare(&mut self, id: u16) {
        self.map.remove(&id);
    }

    /// Resolve a `WireExpr` into the full key expression it names, writing it
    /// into `out`.
    ///
    /// Three shapes, and all three are ordinary:
    ///
    ///  - `scope == 0` — the expression is the suffix, verbatim.
    ///  - `scope != 0`, empty suffix — exactly the mapped expression.
    ///  - `scope != 0`, non-empty suffix — the mapped expression is a *prefix*
    ///    and the suffix completes it, joined with `/` unless the prefix
    ///    already ends in one.
    ///
    /// `None` means the scope names a mapping this session never saw — which
    /// happens when the table filled, or when a peer references an id it
    /// declared before this session attached.
    pub fn resolve<'b>(
        &self,
        wire_expr: &WireExpr<'_>,
        out: &'b mut String<MAX_MAPPED_KEYEXPR>,
    ) -> Option<&'b str> {
        out.clear();

        if wire_expr.scope == 0 {
            out.push_str(wire_expr.suffix).ok()?;
            return Some(out.as_str());
        }

        let prefix = self.map.get(&wire_expr.scope)?;
        out.push_str(prefix.as_str()).ok()?;

        if !wire_expr.suffix.is_empty() {
            if !prefix.ends_with('/') && !wire_expr.suffix.starts_with('/') {
                out.push('/').ok()?;
            }
            out.push_str(wire_expr.suffix).ok()?;
        }

        Some(out.as_str())
    }

    /// How many mappings are held. For the tests, and for a gauge.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(scope: u16, suffix: &str) -> WireExpr<'_> {
        WireExpr {
            scope,
            mapping: Mapping::default(),
            suffix,
        }
    }

    #[test]
    fn an_unmapped_expression_is_its_own_suffix() {
        let table = KeyExprTable::new();
        let mut buf = String::new();
        assert_eq!(
            table.resolve(&wire(0, "fieldblox/org/a/block/b"), &mut buf),
            Some("fieldblox/org/a/block/b")
        );
    }

    /// The shape that broke liveliness: a declaration referencing a mapping
    /// with nothing in the suffix.
    #[test]
    fn a_bare_scope_resolves_to_the_whole_mapping() {
        let mut table = KeyExprTable::new();
        assert!(table.declare(17, "fieldblox/liveness-probe/alpha"));
        let mut buf = String::new();
        assert_eq!(
            table.resolve(&wire(17, ""), &mut buf),
            Some("fieldblox/liveness-probe/alpha")
        );
    }

    #[test]
    fn a_scope_with_a_suffix_joins_them() {
        let mut table = KeyExprTable::new();
        table.declare(3, "fieldblox/org/a");
        let mut buf = String::new();
        assert_eq!(
            table.resolve(&wire(3, "block/b"), &mut buf),
            Some("fieldblox/org/a/block/b")
        );
    }

    /// Joining must not double the separator, or produce a key that matches
    /// nothing.
    #[test]
    fn joining_never_doubles_the_separator() {
        let mut table = KeyExprTable::new();
        table.declare(4, "fieldblox/org/a/");
        let mut buf = String::new();
        assert_eq!(
            table.resolve(&wire(4, "block/b"), &mut buf),
            Some("fieldblox/org/a/block/b")
        );

        table.declare(5, "fieldblox/org/a");
        let mut buf2 = String::new();
        assert_eq!(
            table.resolve(&wire(5, "/block/b"), &mut buf2),
            Some("fieldblox/org/a/block/b")
        );
    }

    /// An id nobody declared resolves to nothing, rather than to a truncated
    /// key that would match the wrong subscribers.
    #[test]
    fn an_unknown_scope_resolves_to_nothing() {
        let table = KeyExprTable::new();
        let mut buf = String::new();
        assert_eq!(table.resolve(&wire(99, "block/b"), &mut buf), None);
    }

    #[test]
    fn undeclaring_forgets_the_mapping() {
        let mut table = KeyExprTable::new();
        table.declare(7, "fieldblox/org/a");
        assert_eq!(table.len(), 1);
        table.undeclare(7);
        assert!(table.is_empty());

        let mut buf = String::new();
        assert_eq!(table.resolve(&wire(7, ""), &mut buf), None);
    }

    /// Full means refused, not evicted: dropping a live mapping would make its
    /// id silently unresolvable, which reads as "no such key".
    #[test]
    fn a_full_table_refuses_rather_than_evicting() {
        let mut table = KeyExprTable::new();
        // Ids are 1-based: zero means "not a reference".
        for i in 1..=MAX_KEYEXPR_MAPPINGS {
            assert!(table.declare(i as u16, "fieldblox/org/a"), "{i} should fit");
        }
        assert!(!table.declare(999, "fieldblox/org/overflow"));

        // The first mapping is still there.
        let mut buf = String::new();
        assert_eq!(table.resolve(&wire(1, ""), &mut buf), Some("fieldblox/org/a"));
    }

    /// A key expression longer than the table's slot is refused whole, not
    /// stored truncated — a truncated key matches the wrong things.
    /// Zero can never be looked up, because `scope == 0` means "the suffix is
    /// the whole expression". Storing one would be storing something
    /// unreachable.
    #[test]
    fn zero_is_not_a_usable_mapping_id() {
        let mut table = KeyExprTable::new();
        assert!(!table.declare(0, "fieldblox/org/a"));
        assert!(table.is_empty());
    }

    #[test]
    fn an_oversized_expression_is_refused_whole() {
        let mut table = KeyExprTable::new();
        let huge = "x".repeat(MAX_MAPPED_KEYEXPR + 1);
        assert!(!table.declare(1, &huge));
        assert!(table.is_empty());
    }
}
