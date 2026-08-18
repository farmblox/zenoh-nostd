//! # Key Expression Mappings
//!
//! The table mapping numeric key-expression ids back to the expressions they
//! stand for.
//!
//! ## Overview
//!
//! A `WireExpr` is not always a string. A peer may declare a long key once and
//! reference it by a numeric `scope` afterwards, carrying only the part that
//! differs in `suffix`:
//!
//! ```text
//!   DeclareKeyExpr { id: 17, wire_expr: "group/alpha/status" }
//!   Declare(DeclareToken { wire_expr: { scope: 17, suffix: "" } })
//! ```
//!
//! Without a table mapping 17 back to its string, that second message cannot
//! be read: `suffix` alone is empty and parsing it fails. This is what an
//! interest's `KEYEXPRS` flag is for — the key-expression declarations are
//! requested precisely so the declarations referencing them can be resolved.
//!
//! - [`KeyExprTable::declare`]: record a mapping from an inbound
//!   `DeclareKeyExpr`
//! - [`KeyExprTable::undeclare`]: forget one
//! - [`KeyExprTable::resolve`]: turn a `WireExpr` into the expression it names
//!
//! ## Capacity
//!
//! Fixed, like every other table here: a peer with no allocator cannot let a
//! router decide how much memory it uses. When full, a new mapping is refused
//! rather than evicting an old one — eviction would make an id that is still
//! in use silently unresolvable, which reads as "that key does not exist"
//! rather than as the resource exhaustion it is.
//!
//! ## Example
//!
//! ```ignore
//! let mut table = KeyExprTable::new();
//! table.declare(17, "group/alpha/status");
//!
//! let mut buf = heapless::String::new();
//! assert_eq!(table.resolve(&wire_expr, &mut buf), Some("group/alpha/status"));
//! ```

use heapless::{FnvIndexMap, String};
use zenoh_proto::{KeyexprError, fields::*, keyexpr};

/// How many mappings one session remembers.
///
/// A power of two, as `FnvIndexMap` requires. Sixteen covers the shapes a
/// client declares interest in — a handful of wildcards, each mapped once —
/// without reserving space a microcontroller does not have.
pub const MAX_KEYEXPR_MAPPINGS: usize = 16;

/// The longest key expression a mapping can hold.
pub const MAX_MAPPED_KEYEXPR: usize = 256;

/// How many live token ids one session remembers.
pub const MAX_LIVELINESS_TOKENS: usize = 16;

/// Numeric key-expression ids to the expressions they stand for.
#[derive(Default)]
pub struct KeyExprTable {
    map: FnvIndexMap<u16, String<MAX_MAPPED_KEYEXPR>, MAX_KEYEXPR_MAPPINGS>,
}

impl KeyExprTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `id -> ke`, from an inbound `DeclareKeyExpr`.
    ///
    /// Returns `false` when the table is full, the expression does not fit, or
    /// the id is zero, so the caller can report it once rather than discovering
    /// it later as an unresolvable reference.
    ///
    /// Zero is not a usable id: `scope == 0` is how a `WireExpr` says it is not
    /// a reference but the literal suffix, so a mapping stored at 0 could never
    /// be looked up.
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

    /// Resolve a `WireExpr` into the key expression it names, writing it into
    /// `out`.
    ///
    /// Three shapes:
    ///
    ///  - `scope == 0` — the expression is the suffix, verbatim.
    ///  - `scope != 0`, empty suffix — exactly the mapped expression.
    ///  - `scope != 0`, non-empty suffix — the mapped expression is a prefix
    ///    and the suffix completes it, joined with `/` unless the prefix
    ///    already ends in one.
    ///
    /// `None` means the scope names a mapping this session never saw: the table
    /// filled, or the peer referenced an id it declared before this session
    /// attached.
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

    /// Resolve and validate a wire expression in one step.
    pub fn resolve_keyexpr<'b>(
        &self,
        wire_expr: &WireExpr<'_>,
        out: &'b mut String<MAX_MAPPED_KEYEXPR>,
    ) -> core::result::Result<Option<&'b keyexpr>, KeyexprError> {
        self.resolve(wire_expr, out).map(keyexpr::new).transpose()
    }

    /// How many mappings are held.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Token ids to the fully resolved expressions they keep alive.
///
/// `UndeclareToken` may carry only an id. Remembering the declaration is the
/// only way to turn that id back into the deletion event subscribers expect.
#[derive(Default)]
pub struct TokenTable {
    map: FnvIndexMap<u32, String<MAX_MAPPED_KEYEXPR>, MAX_LIVELINESS_TOKENS>,
}

impl TokenTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn declare(&mut self, id: u32, keyexpr: &str) -> bool {
        if id == 0 || self.map.contains_key(&id) {
            return false;
        }
        let Ok(keyexpr) = String::try_from(keyexpr) else {
            return false;
        };
        self.map.insert(id, keyexpr).is_ok()
    }

    pub fn undeclare(&mut self, id: u32) -> Option<String<MAX_MAPPED_KEYEXPR>> {
        self.map.remove(&id)
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

    /// A declaration referencing a mapping with nothing in the suffix — the
    /// shape a router answers an interest with.
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

    /// Joining must not double the separator, which would produce a key that
    /// matches nothing.
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

    /// An undeclared id resolves to nothing rather than to a truncated key,
    /// which would match the wrong subscribers.
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
    /// id unresolvable, which reads as "no such key".
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
        assert_eq!(
            table.resolve(&wire(1, ""), &mut buf),
            Some("fieldblox/org/a")
        );
    }

    /// Zero can never be looked up, because `scope == 0` means the suffix is
    /// the whole expression, so storing a mapping there would store something
    /// unreachable.
    #[test]
    fn zero_is_not_a_usable_mapping_id() {
        let mut table = KeyExprTable::new();
        assert!(!table.declare(0, "fieldblox/org/a"));
        assert!(table.is_empty());
    }

    /// A key expression longer than the table's slot is refused whole rather
    /// than stored truncated: a truncated key matches the wrong things.
    #[test]
    fn an_oversized_expression_is_refused_whole() {
        let mut table = KeyExprTable::new();
        let huge = "x".repeat(MAX_MAPPED_KEYEXPR + 1);
        assert!(!table.declare(1, &huge));
        assert!(table.is_empty());
    }

    #[test]
    fn token_ids_round_trip_the_resolved_expression() {
        let mut tokens = TokenTable::new();
        assert!(tokens.declare(12, "fieldblox/org/a/block/b/runtime/owner/p"));
        assert_eq!(
            tokens.undeclare(12).as_deref(),
            Some("fieldblox/org/a/block/b/runtime/owner/p")
        );
        assert!(tokens.undeclare(12).is_none());
    }

    #[test]
    fn token_ids_are_unique_and_nonzero() {
        let mut tokens = TokenTable::new();
        assert!(!tokens.declare(0, "fieldblox/token/zero"));
        assert!(tokens.declare(4, "fieldblox/token/first"));
        assert!(!tokens.declare(4, "fieldblox/token/replacement"));
        assert_eq!(
            tokens.undeclare(4).as_deref(),
            Some("fieldblox/token/first")
        );
    }
}
