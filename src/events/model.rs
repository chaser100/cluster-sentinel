//! Typed cluster event model preserved for agents and metrics.

use chrono::{DateTime, TimeZone, Utc};
use k8s_openapi::api::core::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, Time};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn time_to_utc(time: &Time) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(time.0.as_second(), 0).single()
}

fn micro_time_to_utc(time: &MicroTime) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(time.0.as_second(), 0).single()
}

/// Involved Kubernetes object identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InvolvedObject {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub uid: Option<String>,
    pub api_version: Option<String>,
}

/// Normalized cluster event used by registry/MCP surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClusterEvent {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub resource_version: String,
    pub event_type: String,
    pub reason: String,
    pub message: String,
    pub count: i32,
    pub involved_object: InvolvedObject,
    pub source_component: Option<String>,
    pub first_timestamp: Option<DateTime<Utc>>,
    pub last_timestamp: Option<DateTime<Utc>>,
    pub event_time: Option<DateTime<Utc>>,
    pub registered_at: DateTime<Utc>,
}

impl ClusterEvent {
    /// Best-effort observation time for newest-first ordering and search windows.
    ///
    /// Preference: `last_timestamp` → `event_time` → `first_timestamp` → `registered_at`.
    #[must_use]
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.last_timestamp
            .or(self.event_time)
            .or(self.first_timestamp)
            .unwrap_or(self.registered_at)
    }

    /// Convert a Kubernetes `Event` into a typed model.
    ///
    /// # Errors
    ///
    /// Returns an error string when required identity fields are missing.
    pub fn try_from_kube(event: &Event, registered_at: DateTime<Utc>) -> Result<Self, String> {
        let meta = &event.metadata;
        let uid = meta
            .uid
            .clone()
            .ok_or_else(|| "event missing metadata.uid".to_owned())?;
        let name = meta
            .name
            .clone()
            .ok_or_else(|| "event missing metadata.name".to_owned())?;
        let namespace = meta.namespace.clone().unwrap_or_default();
        let resource_version = meta.resource_version.clone().unwrap_or_default();

        let involved = &event.involved_object;
        Ok(Self {
            uid,
            namespace,
            name,
            resource_version,
            event_type: event.type_.clone().unwrap_or_else(|| "Normal".to_owned()),
            reason: event.reason.clone().unwrap_or_default(),
            message: event.message.clone().unwrap_or_default(),
            count: event.count.unwrap_or(1),
            involved_object: InvolvedObject {
                kind: involved.kind.clone().unwrap_or_default(),
                namespace: involved.namespace.clone().unwrap_or_default(),
                name: involved.name.clone().unwrap_or_default(),
                uid: involved.uid.clone(),
                api_version: involved.api_version.clone(),
            },
            source_component: event
                .source
                .as_ref()
                .and_then(|source| source.component.clone()),
            first_timestamp: event.first_timestamp.as_ref().and_then(time_to_utc),
            last_timestamp: event.last_timestamp.as_ref().and_then(time_to_utc),
            event_time: event.event_time.as_ref().and_then(micro_time_to_utc),
            registered_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    #[test]
    fn maps_kube_event_identity() {
        let registered_at = Utc.with_ymd_and_hms(2026, 9, 2, 7, 0, 0).unwrap();
        let event = Event {
            metadata: ObjectMeta {
                uid: Some("uid-1".into()),
                name: Some("pod.abc".into()),
                namespace: Some("dev".into()),
                resource_version: Some("42".into()),
                ..ObjectMeta::default()
            },
            involved_object: k8s_openapi::api::core::v1::ObjectReference {
                kind: Some("Pod".into()),
                namespace: Some("dev".into()),
                name: Some("web-0".into()),
                uid: Some("pod-uid".into()),
                api_version: Some("v1".into()),
                ..Default::default()
            },
            reason: Some("BackOff".into()),
            message: Some("Back-off restarting failed container".into()),
            type_: Some("Warning".into()),
            count: Some(3),
            ..Default::default()
        };

        let mapped = ClusterEvent::try_from_kube(&event, registered_at).unwrap();
        assert_eq!(mapped.uid, "uid-1");
        assert_eq!(mapped.reason, "BackOff");
        assert_eq!(mapped.involved_object.name, "web-0");
        assert_eq!(mapped.count, 3);
        assert_eq!(mapped.registered_at, registered_at);
    }
}
