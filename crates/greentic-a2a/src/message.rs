//! What travels between agents: messages, their parts, and the tasks a
//! long-running exchange produces.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    #[serde(rename = "ROLE_UNSPECIFIED")]
    Unspecified,
    #[serde(rename = "ROLE_USER")]
    User,
    #[serde(rename = "ROLE_AGENT")]
    Agent,
}

/// One piece of a message.
///
/// The proto models the content as a `oneof`, but `metadata`, `filename` and
/// `media_type` sit OUTSIDE it as ordinary siblings — so the JSON is **flat**:
/// `{"text": "hi", "mediaType": "text/plain"}`, never
/// `{"text": {"text": "hi"}}`.
///
/// An externally-tagged Rust enum would produce that nested form and would
/// silently speak a dialect no agent understands, which is why this is a
/// struct with optional content fields rather than an enum. The cost is that
/// the type does not enforce "exactly one of these is set"; the constructors
/// below are how we only ever emit valid ones.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Part {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Part {
    /// A plain text part — the shape almost every reply uses.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            ..Self::default()
        }
    }

    /// A structured-data part.
    pub fn data(data: serde_json::Value) -> Self {
        Self {
            data: Some(data),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    pub role: Role,
    pub parts: Vec<Part>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Task lifecycle.
///
/// `InputRequired` is the one worth naming: a Greentic flow parked on a card
/// or a form is exactly this state, and most protocols have nowhere to put it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    #[serde(rename = "TASK_STATE_UNSPECIFIED")]
    Unspecified,
    #[serde(rename = "TASK_STATE_SUBMITTED")]
    Submitted,
    #[serde(rename = "TASK_STATE_WORKING")]
    Working,
    #[serde(rename = "TASK_STATE_COMPLETED")]
    Completed,
    #[serde(rename = "TASK_STATE_FAILED")]
    Failed,
    /// One `l`. v1.0 renamed this from the 0.x `cancelled`.
    #[serde(rename = "TASK_STATE_CANCELED")]
    Canceled,
    #[serde(rename = "TASK_STATE_INPUT_REQUIRED")]
    InputRequired,
    #[serde(rename = "TASK_STATE_REJECTED")]
    Rejected,
    #[serde(rename = "TASK_STATE_AUTH_REQUIRED")]
    AuthRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatus {
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<Part>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_text_message_round_trips_in_camel_case() {
        let json = r#"{
          "messageId": "m-1",
          "contextId": "c-1",
          "role": "ROLE_USER",
          "parts": [{ "text": "hello" }]
        }"#;
        let msg: Message = serde_json::from_str(json).expect("message parses");
        assert_eq!(msg.message_id, "m-1");
        assert_eq!(msg.role, Role::User);
        assert_eq!(msg.parts.len(), 1);
        assert_eq!(msg.parts[0].text.as_deref(), Some("hello"));
        let out = serde_json::to_string(&msg).unwrap();
        assert!(out.contains("\"messageId\""));
        assert!(!out.contains("message_id"));
    }

    #[test]
    fn the_cancelled_state_is_spelled_with_one_l() {
        // v1.0 renamed it. A 0.x-era "cancelled" must not parse, or we will
        // silently treat a cancelled task as unknown.
        let state: TaskState = serde_json::from_str(r#""TASK_STATE_CANCELED""#).unwrap();
        assert_eq!(state, TaskState::Canceled);
        assert!(serde_json::from_str::<TaskState>(r#""TASK_STATE_CANCELLED""#).is_err());
    }

    #[test]
    fn input_required_is_modelled_because_our_flows_park() {
        let state: TaskState = serde_json::from_str(r#""TASK_STATE_INPUT_REQUIRED""#).unwrap();
        assert_eq!(state, TaskState::InputRequired);
    }

    #[test]
    fn a_task_carries_its_status_and_history() {
        let json = r#"{
          "id": "t-1",
          "contextId": "c-1",
          "status": { "state": "TASK_STATE_WORKING" },
          "history": [
            { "messageId": "m-1", "role": "ROLE_USER", "parts": [{ "text": "hi" }] }
          ]
        }"#;
        let task: Task = serde_json::from_str(json).expect("task parses");
        assert_eq!(task.id, "t-1");
        assert_eq!(task.status.state, TaskState::Working);
        assert_eq!(task.history.len(), 1);
    }

    #[test]
    fn a_data_part_parses_from_the_flat_wire_shape() {
        let part: Part = serde_json::from_str(r#"{ "data": { "k": 1 } }"#).unwrap();
        assert!(part.data.is_some());
        assert!(part.text.is_none());
    }

    #[test]
    fn a_text_part_serializes_flat_not_nested() {
        // The trap this type exists to avoid: an externally-tagged enum would
        // emit {"text":{"text":"hi"}}, which no agent understands.
        let out = serde_json::to_string(&Part::text("hi")).unwrap();
        assert_eq!(out, r#"{"text":"hi"}"#);
    }

    #[test]
    fn no_struct_emits_a_snake_case_key() {
        let task = Task {
            id: "t-1".into(),
            context_id: Some("c-1".into()),
            status: TaskStatus {
                state: TaskState::Working,
                message: Some(Message {
                    message_id: "m-1".into(),
                    context_id: None,
                    task_id: None,
                    role: Role::Agent,
                    parts: vec![
                        Part::text("hello"),
                        Part::data(serde_json::json!({"k": 1})),
                        Part {
                            text: Some("typed".into()),
                            media_type: Some("text/plain".into()),
                            ..Default::default()
                        },
                    ],
                    metadata: None,
                }),
                timestamp: None,
            },
            artifacts: vec![Artifact {
                artifact_id: Some("a-1".into()),
                name: None,
                parts: vec![Part::text("artifact")],
            }],
            history: vec![],
            metadata: None,
        };
        let value = serde_json::to_value(&task).expect("serialises");
        assert_eq!(
            crate::testutil::first_snake_case_key(&value),
            None,
            "a struct lost rename_all = \"camelCase\""
        );
    }
}
