//! Adapter from agent history to mistl's shared OpenAI-compatible client.

use anyhow::{Result, anyhow, bail};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::config::Config;
use crate::agent::types::{AssistantTurn, Message, Role, ToolSpec};
use crate::ai::openai::{self, ToolOptions, UpstreamConfig, UpstreamHttpError};
use crate::ai::protocol::ChatMessage;

pub fn is_tools_unsupported(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamHttpError>()
        .is_some_and(|e| e.status == 400 && e.detail.contains("tools_unsupported"))
}

pub struct LlmClient {
    upstream: UpstreamConfig,
}

impl LlmClient {
    pub fn new(cfg: &Config) -> Self {
        Self {
            upstream: UpstreamConfig {
                base_url: String::new(),
                api_key: String::new(),
                // The local API treats an empty model as mistl's default.
                model: Some(cfg.model.clone()),
                reasoning_effort: cfg.reasoning_effort.clone(),
            },
        }
    }

    pub fn set_base_url(&mut self, url: String) {
        self.upstream.base_url = url.trim_end_matches('/').to_string();
    }

    pub fn set_model(&mut self, model: String) {
        self.upstream.model = Some(model);
    }

    pub fn set_reasoning_effort(&mut self, effort: Option<String>) {
        self.upstream.reasoning_effort = effort;
    }

    fn upstream(&self) -> Result<&UpstreamConfig> {
        if self.upstream.base_url.is_empty() {
            bail!("mistl's local AI API is not initialized");
        }
        Ok(&self.upstream)
    }

    pub async fn list_models(&self, cancel: &CancellationToken) -> Result<Vec<String>> {
        let upstream = self.upstream()?;
        let mut models = tokio::select! {
            biased;
            _ = cancel.cancelled() => bail!("cancelled"),
            result = openai::fetch_models(upstream) => result?,
        };
        models.sort();
        models.dedup();
        Ok(models)
    }

    pub async fn complete(
        &self,
        messages: &[Message],
        tools: &[ToolSpec],
        on_delta: &mut (dyn FnMut(&str) + Send),
        cancel: &CancellationToken,
    ) -> Result<AssistantTurn> {
        let upstream = self.upstream()?;
        let messages = messages
            .iter()
            .map(chat_message)
            .collect::<Result<Vec<_>>>()?;
        let options = ToolOptions {
            tools: if tools.is_empty() {
                None
            } else {
                Some(serde_json::to_value(tools)?)
            },
            ..ToolOptions::default()
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let request =
            openai::stream_chat_completion_tools(upstream, &messages, None, &options, Some(tx));
        tokio::pin!(request);
        let mut deltas_open = true;
        let result = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => bail!("cancelled"),
                delta = rx.recv(), if deltas_open => match delta {
                    Some(delta) => on_delta(&delta),
                    None => deltas_open = false,
                },
                result = &mut request => break result,
            }
        };
        // A completion can finish in the same poll that emits its last delta.
        while let Ok(delta) = rx.try_recv() {
            on_delta(&delta);
        }
        let output = result?;
        Ok(AssistantTurn {
            content: output.content,
            tool_calls: output
                .tool_calls
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| anyhow!("invalid tool calls from mistl's AI API: {e}"))?
                .unwrap_or_default(),
        })
    }
}

fn chat_message(message: &Message) -> Result<ChatMessage> {
    Ok(ChatMessage {
        role: match message.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
        }
        .into(),
        content: message.content.clone().unwrap_or_default(),
        tool_calls: if message.tool_calls.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&message.tool_calls)?)
        },
        tool_call_id: message.tool_call_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::types::{FunctionCall, ToolCall};
    use serde_json::{Value, json};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    async fn endpoint(body: Value) -> (String, tokio::task::JoinHandle<(String, Value)>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            let request_line = line.trim().to_string();
            let mut length = 0;
            loop {
                line.clear();
                stream.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            assert!(length < 64 * 1024);
            let mut request = vec![0; length];
            stream.read_exact(&mut request).await.unwrap();
            let request = if request.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&request).unwrap()
            };
            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            );
            stream
                .get_mut()
                .write_all(response.as_bytes())
                .await
                .unwrap();
            (request_line, request)
        });
        (url, task)
    }

    #[tokio::test]
    async fn completion_preserves_defaults_tools_and_delivers_final_delta_once() {
        let calls = json!([{"id":"one","type":"function","function":{
            "name":"mistl","arguments":"{\"args\":[\"status\"]}"
        }}]);
        let (url, request) = endpoint(json!({"choices":[{"message":{
            "content":"checking", "tool_calls": calls
        }}]}))
        .await;
        let mut client = LlmClient::new(&Config::default());
        client.set_base_url(url);
        let mut text = String::new();
        let output = client
            .complete(
                &[Message::user("status")],
                &crate::agent::mistl_tool::tool_specs(),
                &mut |delta| text.push_str(delta),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(text, "checking");
        assert_eq!(output.content, text);
        assert_eq!(serde_json::to_value(output.tool_calls).unwrap(), calls);
        let (path, body) = request.await.unwrap();
        assert!(path.starts_with("POST /v1/chat/completions "));
        assert_eq!(body["model"], "");
        assert!(body.get("reasoning_effort").is_none());
        assert_eq!(body["tools"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["messages"],
            json!([{"role":"user","content":"status"}])
        );
    }

    #[tokio::test]
    async fn model_catalog_is_sorted_and_deduplicated() {
        let (url, request) = endpoint(json!({"data":[{"id":"z"},{"id":"a"},{"id":"z"}]})).await;
        let mut client = LlmClient::new(&Config::default());
        client.set_base_url(url);
        assert_eq!(
            client.list_models(&CancellationToken::new()).await.unwrap(),
            ["a", "z"]
        );
        assert!(request.await.unwrap().0.starts_with("GET /v1/models "));
    }

    #[tokio::test]
    async fn cancelled_completion_does_not_issue_a_request() {
        let mut client = LlmClient::new(&Config::default());
        client.set_base_url("http://127.0.0.1:1/v1".into());
        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = client
            .complete(&[], &[], &mut |_| {}, &cancel)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "cancelled");
    }

    #[test]
    fn converts_native_history_without_losing_tool_fields() {
        let calls = vec![ToolCall {
            id: "call_1".into(),
            kind: "function".into(),
            function: FunctionCall {
                name: "mistl".into(),
                arguments: r#"{"args":["status"]}"#.into(),
            },
        }];
        let message = Message {
            role: Role::Assistant,
            content: None,
            tool_calls: calls.clone(),
            tool_call_id: None,
        };
        let converted = chat_message(&message).unwrap();
        assert_eq!(converted.role, "assistant");
        assert_eq!(converted.content, "");
        assert_eq!(
            converted.tool_calls,
            Some(serde_json::to_value(calls).unwrap())
        );
        let result = chat_message(&Message::tool_result("call_1", "ok")).unwrap();
        assert_eq!(result.role, "tool");
        assert_eq!(result.tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(result.content, "ok");
        assert!(
            chat_message(&Message::user("hi"))
                .unwrap()
                .tool_calls
                .is_none()
        );
    }

    #[test]
    fn unsupported_tools_requires_typed_http_400() {
        for (status, detail, expected) in [
            (400, "tools_unsupported", true),
            (400, "other", false),
            (502, "tools_unsupported", false),
        ] {
            let error = anyhow::Error::new(UpstreamHttpError {
                label: "test",
                status,
                detail: detail.into(),
            })
            .context("request failed");
            assert_eq!(is_tools_unsupported(&error), expected);
        }
        assert!(!is_tools_unsupported(&anyhow!("tools_unsupported")));
    }
}
