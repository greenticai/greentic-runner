//! The JSON-RPC 2.0 envelope and the four methods the MVP surface uses.
//!
//! **The method names are PascalCase.** `SendMessage`, not `message/send` —
//! the slash form is the 0.x spelling, and `/message:send` is the HTTP
//! binding, not the RPC name. Every 0.x-era tutorial gets this wrong.

use serde::{Deserialize, Serialize};

use crate::message::{Message, Task};

pub const METHOD_SEND_MESSAGE: &str = "SendMessage";
pub const METHOD_GET_TASK: &str = "GetTask";
pub const METHOD_CANCEL_TASK: &str = "CancelTask";
pub const METHOD_LIST_TASKS: &str = "ListTasks";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest<P> {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    pub params: P,
}

impl<P> JsonRpcRequest<P> {
    /// Build a request with the protocol's fixed `jsonrpc` value, so no call
    /// site has to remember it.
    pub fn new(id: u64, method: &str, params: P) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcResponse<R> {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<R>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageParams {
    pub message: Message,
    /// Echoed from `AgentInterface.tenant` when the card set one — the spec
    /// makes that a MUST, not a courtesy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

/// What `SendMessage` answers with.
///
/// An agent MAY reply with a `Message` directly, or with a `Task` the caller
/// then polls. Both are conformant, so both are modelled — a client that
/// handles only one fails against half the agents in the wild.
#[derive(Debug, Clone, PartialEq)]
pub enum SendMessageResult {
    Message(Message),
    Task(Task),
}

impl Serialize for SendMessageResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        match self {
            SendMessageResult::Message(m) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("message", m)?;
                map.end()
            }
            SendMessageResult::Task(t) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("task", t)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for SendMessageResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::MapAccess;
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SendMessageResult;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an object with either 'message' or 'task' key")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                if let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                    match key.as_str() {
                        "message" => {
                            let message: Message =
                                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                            Ok(SendMessageResult::Message(message))
                        }
                        "task" => {
                            let task: Task =
                                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                            Ok(SendMessageResult::Task(task))
                        }
                        _ => Err(serde::de::Error::custom("expected 'message' or 'task' key")),
                    }
                } else {
                    Err(serde::de::Error::custom("expected non-empty object"))
                }
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTaskParams {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_length: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelTaskParams {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTasksResult {
    #[serde(default)]
    pub tasks: Vec<Task>,
    /// Always present; an empty string means this was the final page.
    #[serde(default)]
    pub next_page_token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Part, Role, TaskState};

    #[test]
    fn the_method_names_are_pascal_case_not_the_zero_x_slash_form() {
        assert_eq!(METHOD_SEND_MESSAGE, "SendMessage");
        assert_eq!(METHOD_GET_TASK, "GetTask");
        assert_eq!(METHOD_CANCEL_TASK, "CancelTask");
        assert_eq!(METHOD_LIST_TASKS, "ListTasks");
    }

    #[test]
    fn send_message_accepts_a_direct_message_reply() {
        let json = r#"{
          "jsonrpc": "2.0", "id": 1,
          "result": {
            "message": {
              "messageId": "m-2", "role": "ROLE_AGENT",
              "parts": [{ "text": "here you go" }]
            }
          }
        }"#;
        let resp: JsonRpcResponse<SendMessageResult> = serde_json::from_str(json).unwrap();
        let result = resp.result.expect("a result, not an error");
        match result {
            SendMessageResult::Message(m) => {
                assert_eq!(m.role, Role::Agent);
                assert_eq!(m.parts[0].text.as_deref(), Some("here you go"));
            }
            SendMessageResult::Task(_) => panic!("expected a Message reply"),
        }
    }

    #[test]
    fn send_message_also_accepts_a_task_reply() {
        // An agent MAY answer with a Task instead, and a client that models
        // only one of the two fails against half the agents in the wild.
        let json = r#"{
          "jsonrpc": "2.0", "id": 1,
          "result": { "task": { "id": "t-9", "status": { "state": "TASK_STATE_SUBMITTED" } } }
        }"#;
        let resp: JsonRpcResponse<SendMessageResult> = serde_json::from_str(json).unwrap();
        match resp.result.unwrap() {
            SendMessageResult::Task(t) => {
                assert_eq!(t.id, "t-9");
                assert_eq!(t.status.state, TaskState::Submitted);
            }
            SendMessageResult::Message(_) => panic!("expected a Task reply"),
        }
    }

    #[test]
    fn an_error_response_carries_code_and_message() {
        let json = r#"{
          "jsonrpc": "2.0", "id": 1,
          "error": { "code": -32004, "message": "unsupported operation" }
        }"#;
        let resp: JsonRpcResponse<SendMessageResult> = serde_json::from_str(json).unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.expect("an error");
        assert_eq!(err.code, -32004);
        assert_eq!(err.message, "unsupported operation");
    }

    #[test]
    fn a_request_serializes_with_the_tenant_the_card_asked_us_to_echo() {
        let req = JsonRpcRequest::new(
            1,
            METHOD_SEND_MESSAGE,
            SendMessageParams {
                message: crate::message::Message {
                    message_id: "m-1".into(),
                    context_id: None,
                    task_id: None,
                    role: Role::User,
                    parts: vec![Part::text("hi")],
                    metadata: None,
                },
                tenant: Some("acme".into()),
            },
        );
        let out = serde_json::to_string(&req).unwrap();
        assert!(out.contains("\"method\":\"SendMessage\""));
        assert!(out.contains("\"tenant\":\"acme\""));
        assert!(out.contains("\"jsonrpc\":\"2.0\""));
    }

    #[test]
    fn no_struct_emits_a_snake_case_key() {
        // GetTaskParams with all fields populated
        let get_task = GetTaskParams {
            id: "t-1".into(),
            tenant: Some("tenant-1".into()),
            history_length: Some(10),
        };
        let value = serde_json::to_value(&get_task).expect("serialises");
        assert_eq!(
            crate::testutil::first_snake_case_key(&value),
            None,
            "GetTaskParams lost rename_all = \"camelCase\""
        );

        // ListTasksParams with all fields populated
        let list_tasks = ListTasksParams {
            tenant: Some("tenant-1".into()),
            page_token: Some("next".into()),
        };
        let value = serde_json::to_value(&list_tasks).expect("serialises");
        assert_eq!(
            crate::testutil::first_snake_case_key(&value),
            None,
            "ListTasksParams lost rename_all = \"camelCase\""
        );

        // ListTasksResult with all fields populated
        let list_result = ListTasksResult {
            tasks: vec![],
            next_page_token: "token".into(),
        };
        let value = serde_json::to_value(&list_result).expect("serialises");
        assert_eq!(
            crate::testutil::first_snake_case_key(&value),
            None,
            "ListTasksResult lost rename_all = \"camelCase\""
        );
    }
}
