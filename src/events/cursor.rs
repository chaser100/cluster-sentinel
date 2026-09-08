//! Versioned opaque pagination cursors for event search.

use chrono::{DateTime, Utc};

use super::model::ClusterEvent;

/// Parsed `search_events` cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedCursor {
    /// First page.
    Start,
    /// Deprecated decimal offset into the filtered result set.
    Offset(usize),
    /// Stable keyset cursor keyed by `(observed_at, event_uid)` DESC order.
    Keyset {
        observed_at: DateTime<Utc>,
        event_uid: String,
    },
}

/// Encode a keyset cursor (`v1:<rfc3339>|<event_uid>`).
#[must_use]
pub fn encode_keyset_cursor(observed_at: DateTime<Utc>, event_uid: &str) -> String {
    format!(
        "v1:{}|{}",
        observed_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        event_uid
    )
}

/// Encode keyset cursor from the last event of a page.
#[must_use]
pub fn encode_keyset_from_event(event: &ClusterEvent) -> String {
    encode_keyset_cursor(event.observed_at(), &event.uid)
}

/// Parse a client cursor.
///
/// Accepts:
/// - empty / missing → start
/// - decimal digits → deprecated offset
/// - `v1:<rfc3339>|<uid>` → keyset
///
/// # Errors
///
/// Returns a human-readable reason when the token is malformed.
pub fn parse_cursor(raw: Option<&str>) -> Result<ParsedCursor, String> {
    match raw {
        None => Ok(ParsedCursor::Start),
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(ParsedCursor::Start);
            }
            if trimmed.bytes().all(|b| b.is_ascii_digit()) {
                return trimmed
                    .parse::<usize>()
                    .map(ParsedCursor::Offset)
                    .map_err(|_| format!("invalid cursor '{trimmed}' (expected decimal offset)"));
            }
            let Some(rest) = trimmed.strip_prefix("v1:") else {
                return Err(format!(
                    "invalid cursor '{trimmed}' (expected v1 keyset or decimal offset)"
                ));
            };
            let Some((ts, uid)) = rest.split_once('|') else {
                return Err(format!(
                    "invalid cursor '{trimmed}' (expected v1:<rfc3339>|<event_uid>)"
                ));
            };
            if uid.is_empty() {
                return Err(format!(
                    "invalid cursor '{trimmed}' (empty event_uid in keyset)"
                ));
            }
            let observed_at = DateTime::parse_from_rfc3339(ts)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|_| format!("invalid cursor '{trimmed}' (bad observed_at in keyset)"))?;
            Ok(ParsedCursor::Keyset {
                observed_at,
                event_uid: uid.to_owned(),
            })
        }
    }
}

/// True when `event` is strictly after `cursor` in DESC `(observed_at, event_uid)` order.
#[must_use]
pub fn is_after_keyset(event: &ClusterEvent, observed_at: DateTime<Utc>, event_uid: &str) -> bool {
    let at = event.observed_at();
    at < observed_at || (at == observed_at && event.uid.as_str() < event_uid)
}

/// Slice a DESC-sorted filtered set using keyset (`after`) or deprecated `offset`.
#[must_use]
pub fn paginate_events(
    matched: Vec<ClusterEvent>,
    offset: usize,
    after: Option<&(DateTime<Utc>, String)>,
    limit: usize,
) -> super::registry::EventSearchResult {
    let matched_count = matched.len();
    let page_source: Vec<ClusterEvent> = if let Some((observed_at, event_uid)) = after {
        matched
            .into_iter()
            .filter(|event| is_after_keyset(event, *observed_at, event_uid))
            .collect()
    } else {
        let start = offset.min(matched_count);
        matched[start..].to_vec()
    };
    let end = limit.min(page_source.len());
    let truncated = end < page_source.len();
    let events = page_source[..end].to_vec();
    let returned = events.len();
    let next_after = truncated
        .then(|| {
            events
                .last()
                .map(|event| (event.observed_at(), event.uid.clone()))
        })
        .flatten();
    let next_offset = if after.is_some() {
        None
    } else {
        truncated.then_some(offset.saturating_add(returned))
    };
    super::registry::EventSearchResult {
        events,
        matched: matched_count,
        returned,
        truncated,
        next_offset,
        next_after,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::InvolvedObject;

    fn sample(uid: &str, at: DateTime<Utc>) -> ClusterEvent {
        ClusterEvent {
            uid: uid.into(),
            namespace: "dev".into(),
            name: uid.into(),
            resource_version: "1".into(),
            event_type: "Warning".into(),
            reason: "BackOff".into(),
            message: "msg".into(),
            count: 1,
            involved_object: InvolvedObject {
                kind: "Pod".into(),
                namespace: "dev".into(),
                name: "web".into(),
                uid: None,
                api_version: None,
            },
            source_component: None,
            first_timestamp: None,
            last_timestamp: Some(at),
            event_time: None,
            registered_at: at,
        }
    }

    #[test]
    fn roundtrips_keyset_cursor() {
        let at = DateTime::parse_from_rfc3339("2026-09-08T09:00:00.123456Z")
            .expect("ts")
            .with_timezone(&Utc);
        let encoded = encode_keyset_cursor(at, "evt-1");
        assert_eq!(
            parse_cursor(Some(&encoded)).expect("parse"),
            ParsedCursor::Keyset {
                observed_at: at,
                event_uid: "evt-1".into(),
            }
        );
    }

    #[test]
    fn parses_deprecated_offset() {
        assert_eq!(
            parse_cursor(Some("42")).expect("parse"),
            ParsedCursor::Offset(42)
        );
        assert_eq!(parse_cursor(None).expect("parse"), ParsedCursor::Start);
    }

    #[test]
    fn keyset_order_is_exclusive_desc() {
        let newer = DateTime::parse_from_rfc3339("2026-09-08T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let older = DateTime::parse_from_rfc3339("2026-09-08T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let cursor_event = sample("b", newer);
        assert!(!is_after_keyset(
            &cursor_event,
            cursor_event.observed_at(),
            &cursor_event.uid
        ));
        assert!(is_after_keyset(&sample("a", older), newer, "b"));
        assert!(is_after_keyset(&sample("a", newer), newer, "b"));
        assert!(!is_after_keyset(&sample("c", newer), newer, "b"));
    }
}
