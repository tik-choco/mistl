//! NIP-01 subscription filters.
//!
//! Each present condition must match; values within a condition are
//! alternatives. Input lists are capped before their values are copied.

use std::collections::BTreeMap;

use serde_json::Value;

use super::event::{Event, is_lower_hex};

/// Maximum number of values in each filter list.
pub const MAX_FILTER_VALUES: usize = 1_000;
/// Maximum size of a tag filter value, in bytes.
const MAX_TAG_VALUE_BYTES: usize = 1_024;

/// A parsed NIP-01 subscription filter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    /// Accepted full event ids.
    pub ids: Option<Vec<String>>,
    /// Accepted author public keys.
    pub authors: Option<Vec<String>>,
    /// Accepted event kinds.
    pub kinds: Option<Vec<u32>>,
    /// `#<letter>` tag conditions, keyed by the single letter.
    pub tags: BTreeMap<char, Vec<String>>,
    /// Inclusive minimum creation timestamp.
    pub since: Option<u64>,
    /// Inclusive maximum creation timestamp.
    pub until: Option<u64>,
    /// Maximum returned events; applied by the store, not by `matches`.
    pub limit: Option<usize>,
}

fn list<'a>(value: &'a Value, name: &str) -> Result<&'a [Value], String> {
    let values = value
        .as_array()
        .ok_or_else(|| format!("{name} must be an array"))?;
    if values.len() > MAX_FILTER_VALUES {
        return Err(format!("{name} has too many values"));
    }
    Ok(values)
}

fn hex_list(value: &Value, name: &str) -> Result<Vec<String>, String> {
    list(value, name)?
        .iter()
        .map(|item| {
            let text = item
                .as_str()
                .filter(|text| is_lower_hex(text, 64))
                .ok_or_else(|| format!("{name} must contain 64-character lowercase hex strings"))?;
            Ok(text.to_owned())
        })
        .collect()
}

fn tag_list(value: &Value, name: &str) -> Result<Vec<String>, String> {
    list(value, name)?
        .iter()
        .map(|item| {
            let text = item
                .as_str()
                .filter(|text| text.len() <= MAX_TAG_VALUE_BYTES)
                .ok_or_else(|| format!("{name} must contain strings of at most 1024 bytes"))?;
            Ok(text.to_owned())
        })
        .collect()
}

fn non_negative_integer(value: &Value, name: &str) -> Result<u64, String> {
    value
        .as_u64()
        .ok_or_else(|| format!("{name} must be a non-negative integer"))
}

impl Filter {
    /// Parses supported NIP-01 fields, ignoring unknown keys.
    pub fn parse(value: &Value) -> Result<Filter, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "filter must be an object".to_owned())?;
        let mut filter = Filter::default();
        for (name, value) in object {
            match name.as_str() {
                "ids" => filter.ids = Some(hex_list(value, name)?),
                "authors" => filter.authors = Some(hex_list(value, name)?),
                "kinds" => {
                    filter.kinds = Some(
                        list(value, name)?
                            .iter()
                            .map(|item| {
                                item.as_u64()
                                    .filter(|kind| *kind <= 65_535)
                                    .map(|kind| kind as u32)
                                    .ok_or_else(|| {
                                        "kinds must contain integers from 0 to 65535".to_owned()
                                    })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
                "since" => filter.since = Some(non_negative_integer(value, name)?),
                "until" => filter.until = Some(non_negative_integer(value, name)?),
                "limit" => {
                    let limit = non_negative_integer(value, name)?;
                    filter.limit =
                        Some(usize::try_from(limit).map_err(|_| "limit is too large".to_owned())?);
                }
                _ if name.len() == 2
                    && name.as_bytes()[0] == b'#'
                    && name.as_bytes()[1].is_ascii_alphabetic() =>
                {
                    let letter = char::from(name.as_bytes()[1]);
                    filter.tags.insert(letter, tag_list(value, name)?);
                }
                _ => {}
            }
        }
        Ok(filter)
    }

    /// Tests all conditions against an event; `limit` does not affect matching.
    pub fn matches(&self, ev: &Event) -> bool {
        self.ids.as_ref().is_none_or(|ids| ids.contains(&ev.id))
            && self
                .authors
                .as_ref()
                .is_none_or(|authors| authors.contains(&ev.pubkey))
            && self
                .kinds
                .as_ref()
                .is_none_or(|kinds| kinds.contains(&ev.kind))
            && self.since.is_none_or(|since| ev.created_at >= since)
            && self.until.is_none_or(|until| ev.created_at <= until)
            && self.tags.iter().all(|(letter, values)| {
                let mut buffer = [0; 4];
                let name = letter.encode_utf8(&mut buffer);
                ev.tag_values(name)
                    .any(|tag_value| values.iter().any(|value| value == tag_value))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::{test_event, test_pubkey};
    use serde_json::json;

    fn sample_event() -> Event {
        test_event(
            &[1; 32],
            100,
            42,
            vec![
                vec!["p".into(), "alice".into(), "ignored".into()],
                vec!["p".into(), "bob".into()],
                vec!["e".into(), "thread".into()],
                vec!["P".into(), "upper".into()],
                vec!["x".into()],
            ],
            "hello",
        )
    }

    #[test]
    fn matches_ids_authors_and_kinds() {
        let event = sample_event();
        let other_id = "f".repeat(64);
        let other_author = test_pubkey(&[2; 32]);
        for (raw, expected) in [
            (json!({}), true),
            (json!({"ids": [other_id, event.id]}), true),
            (json!({"ids": ["f".repeat(64)]}), false),
            (json!({"authors": [other_author, event.pubkey]}), true),
            (json!({"authors": [test_pubkey(&[2; 32])]}), false),
            (json!({"kinds": [1, 42]}), true),
            (json!({"kinds": [1, 2]}), false),
            (
                json!({"ids": [event.id], "authors": [event.pubkey], "kinds": [42]}),
                true,
            ),
            (
                json!({"ids": [event.id], "authors": [event.pubkey], "kinds": [1]}),
                false,
            ),
        ] {
            assert_eq!(
                Filter::parse(&raw).unwrap().matches(&event),
                expected,
                "{raw}"
            );
        }
    }

    #[test]
    fn matches_inclusive_timestamps_and_ignores_limit() {
        let event = sample_event();
        for (raw, expected) in [
            (json!({"since": 100, "until": 100}), true),
            (json!({"since": 99, "until": 101}), true),
            (json!({"since": 101}), false),
            (json!({"until": 99}), false),
            (json!({"since": 101, "until": 99}), false),
            (json!({"limit": 0}), true),
        ] {
            assert_eq!(
                Filter::parse(&raw).unwrap().matches(&event),
                expected,
                "{raw}"
            );
        }
    }

    #[test]
    fn matches_tags_with_or_within_and_across_names() {
        let event = sample_event();
        for (raw, expected) in [
            (json!({"#p": ["carol", "bob"]}), true),
            (json!({"#p": ["alice"], "#e": ["thread"]}), true),
            (json!({"#p": ["alice"], "#e": ["other"]}), false),
            (json!({"#p": ["ignored"]}), false),
            (json!({"#P": ["upper"]}), true),
            (json!({"#p": ["upper"]}), false),
            (json!({"#x": [""]}), false),
            (json!({"#z": ["anything"]}), false),
        ] {
            assert_eq!(
                Filter::parse(&raw).unwrap().matches(&event),
                expected,
                "{raw}"
            );
        }
    }

    #[test]
    fn empty_lists_match_nothing() {
        let event = sample_event();
        for raw in [
            json!({"ids": []}),
            json!({"authors": []}),
            json!({"kinds": []}),
            json!({"#p": []}),
        ] {
            assert!(!Filter::parse(&raw).unwrap().matches(&event), "{raw}");
        }
    }

    #[test]
    fn rejects_invalid_filter_fields() {
        assert!(Filter::parse(&json!([])).is_err());
        for raw in [
            json!({"ids": "not an array"}),
            json!({"ids": ["A".repeat(64)]}),
            json!({"ids": ["0".repeat(63)]}),
            json!({"ids": [1]}),
            json!({"authors": ["g".repeat(64)]}),
            json!({"authors": null}),
            json!({"kinds": [65_536]}),
            json!({"kinds": [-1]}),
            json!({"kinds": [1.0]}),
            json!({"kinds": "42"}),
            json!({"#a": [1]}),
            json!({"#a": "value"}),
            json!({"#a": ["é".repeat(513)]}),
            json!({"since": -1}),
            json!({"since": 1.0}),
            json!({"until": "1"}),
            json!({"limit": null}),
            json!({"limit": -1}),
            json!({"limit": 1.0}),
        ] {
            let error = Filter::parse(&raw).expect_err(&raw.to_string());
            assert!(!error.starts_with("invalid:"), "{error}");
        }
    }

    #[test]
    fn list_and_tag_size_boundaries() {
        for key in ["ids", "authors", "kinds", "#a"] {
            let item = match key {
                "ids" | "authors" => json!("0".repeat(64)),
                "kinds" => json!(0),
                _ => json!("x"),
            };
            let mut raw = json!({});
            raw[key] = json!(vec![item.clone(); MAX_FILTER_VALUES]);
            assert!(Filter::parse(&raw).is_ok(), "{key}");
            raw[key] = json!(vec![item; MAX_FILTER_VALUES + 1]);
            assert!(Filter::parse(&raw).is_err(), "{key}");
        }
        assert!(Filter::parse(&json!({"#a": ["é".repeat(512)]})).is_ok());
        assert!(Filter::parse(&json!({"#a": ["é".repeat(513)]})).is_err());
        assert!(Filter::parse(&json!({"kinds": [0, 65_535]})).is_ok());
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let filter = Filter::parse(&json!({
            "search": "words",
            "#ab": 1,
            "#1": 2,
            "#é": false,
            "other": null
        }))
        .unwrap();
        assert_eq!(filter, Filter::default());
        assert!(filter.matches(&sample_event()));
    }
}
