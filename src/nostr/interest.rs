//! What the cluster's clients want from external relays ("interest").
//!
//! The gateway mirrors client subscriptions instead of pulling whole kinds:
//! only filters whose kinds are all in `[nostr] mirror_kinds` (by default
//! mistlib's signaling kinds) are collected -- from local WebSocket clients
//! and, via the cluster, from followers' clients -- and merged into a few
//! broad filters the gateway subscribes with upstream.
//!
//! Merging is a superset: filters with the same `kinds` and the same set of
//! tag letters (and no ids/authors) become one filter with the union of each
//! tag's values. That can let a few extra events through (mistlib's
//! `#d`x`#p` cross product), never fewer, and it keeps the upstream REQ at a
//! handful of filters however many rooms and nodes there are. `since`,
//! `until` and `limit` are dropped -- mirrored kinds are live traffic.

use std::collections::{BTreeMap, BTreeSet};

use super::filter::Filter;

/// Merged filters kept at most; extra groups are dropped (logged by callers).
pub const MAX_FILTERS: usize = 8;
/// Values per tag letter in one merged filter: the parser's own cap, so a
/// merged filter always re-parses (over the mesh and on upstream relays).
pub const MAX_TAG_VALUES: usize = super::filter::MAX_FILTER_VALUES;

/// Whether `filter` is mirrored: it names kinds and all of them are in
/// `kinds`, and it does not pin ids (a one-off lookup, not a feed).
pub fn mirrored(filter: &Filter, kinds: &[u32]) -> bool {
    filter.ids.is_none()
        && filter
            .kinds
            .as_ref()
            .is_some_and(|k| !k.is_empty() && k.iter().all(|kind| kinds.contains(kind)))
}

type GroupKey = (Vec<u32>, Option<Vec<String>>, Vec<char>);

/// Merges mirrored filters into at most [`MAX_FILTERS`] superset filters,
/// deterministically ordered (so an unchanged interest compares equal).
pub fn merge<'a>(filters: impl IntoIterator<Item = &'a Filter>) -> Vec<Filter> {
    let mut groups: BTreeMap<GroupKey, BTreeMap<char, BTreeSet<String>>> = BTreeMap::new();
    for filter in filters {
        let mut kinds = filter.kinds.clone().unwrap_or_default();
        kinds.sort_unstable();
        kinds.dedup();
        let authors = filter.authors.clone().map(|mut a| {
            a.sort();
            a.dedup();
            a
        });
        let letters: Vec<char> = filter.tags.keys().copied().collect();
        let tags = groups.entry((kinds, authors, letters)).or_default();
        for (letter, values) in &filter.tags {
            let set = tags.entry(*letter).or_default();
            for value in values {
                if set.len() < MAX_TAG_VALUES {
                    set.insert(value.clone());
                }
            }
        }
    }
    groups
        .into_iter()
        .take(MAX_FILTERS)
        .map(|((kinds, authors, _), tags)| Filter {
            kinds: Some(kinds),
            authors,
            tags: tags
                .into_iter()
                .map(|(letter, values)| (letter, values.into_iter().collect()))
                .collect(),
            ..Filter::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn f(value: serde_json::Value) -> Filter {
        Filter::parse(&value).unwrap()
    }

    #[test]
    fn only_filters_within_the_mirrored_kinds_count() {
        let kinds = [25049, 25050];
        assert!(mirrored(&f(json!({"kinds": [25049]})), &kinds));
        assert!(mirrored(
            &f(json!({"kinds": [25049, 25050], "#d": ["x"]})),
            &kinds
        ));
        assert!(!mirrored(&f(json!({"kinds": [25049, 1]})), &kinds));
        assert!(!mirrored(&f(json!({})), &kinds));
        assert!(!mirrored(&f(json!({"kinds": []})), &kinds));
        assert!(!mirrored(
            &f(json!({"kinds": [25050], "ids": ["a".repeat(64)]})),
            &kinds
        ));
    }

    #[test]
    fn mistlib_style_filters_collapse_to_one_per_kind() {
        let filters = [
            f(json!({"kinds": [25049], "#d": ["r1a", "r1b"], "since": 5})),
            f(json!({"kinds": [25049], "#d": ["r2a"]})),
            f(json!({"kinds": [25050], "#d": ["r1a"], "#p": ["pk1", "s1"]})),
            f(json!({"kinds": [25050], "#d": ["r2a"], "#p": ["pk2", "s2"], "limit": 3})),
            f(json!({"kinds": [25050], "#d": ["r1a"], "#p": ["pk1", "s1"]})),
        ];
        let merged = merge(&filters);
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged[0],
            f(json!({"kinds": [25049], "#d": ["r1a", "r1b", "r2a"]}))
        );
        assert_eq!(
            merged[1],
            f(json!({"kinds": [25050], "#d": ["r1a", "r2a"], "#p": ["pk1", "pk2", "s1", "s2"]}))
        );
        // Order of input does not matter.
        let mut reversed = filters.to_vec();
        reversed.reverse();
        assert_eq!(merge(&reversed), merged);
    }

    #[test]
    fn different_tag_letters_or_authors_stay_separate_and_output_is_bounded() {
        let filters = [
            f(json!({"kinds": [25050], "#p": ["a"]})),
            f(json!({"kinds": [25050], "#d": ["a"]})),
            f(json!({"kinds": [25050], "authors": ["b".repeat(64)], "#d": ["a"]})),
        ];
        assert_eq!(merge(&filters).len(), 3);
        let many: Vec<Filter> = (0..20)
            .map(|i| f(json!({"kinds": [25049], "authors": [format!("{i:064x}")]})))
            .collect();
        assert_eq!(merge(&many).len(), MAX_FILTERS);
        let wide: Vec<Filter> = (0..2000)
            .map(|i| f(json!({"kinds": [25049], "#d": [format!("{i}")]})))
            .collect();
        assert_eq!(merge(&wide)[0].tags[&'d'].len(), MAX_TAG_VALUES);
    }
}
