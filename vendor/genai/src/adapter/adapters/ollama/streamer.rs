use crate::adapter::adapters::support::{StreamerCapturedData, StreamerOptions};
use crate::adapter::inter_stream::{InterStreamEnd, InterStreamEvent};
use crate::chat::{ChatOptionsSet, StopReason, ToolCall, Usage};
use crate::webc::WebStream;
use crate::{Error, ModelIden, Result};
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};
use value_ext::JsonValueExt;

/// Ollama streamer for `application/x-ndjson` response.
///
/// Ref: <https://github.com/ollama/ollama/blob/main/docs/api.md#generate-a-chat-completion>
pub struct OllamaStreamer {
	inner: WebStream,
	options: StreamerOptions,

	// -- Set by the poll_next
	/// Flag to prevent polling after a done event
	done: bool,

	captured_data: StreamerCapturedData,
}

impl OllamaStreamer {
	pub fn new(inner: WebStream, model_iden: ModelIden, options_set: ChatOptionsSet<'_, '_>) -> Self {
		Self {
			inner,
			done: false,
			options: StreamerOptions::new(model_iden, options_set),
			captured_data: Default::default(),
		}
	}

	fn capture_tool_calls(&mut self, tool_calls: &[ToolCall]) -> Result<()> {
		if self.options.capture_tool_calls {
			for tool_call in tool_calls {
				self.captured_data.push_tool_call(tool_call.clone())?;
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod security_tests {
	use super::*;
	use crate::adapter::AdapterKind;
	use crate::adapter::adapters::support::MAX_CAPTURED_TOOL_CALLS;
	use crate::chat::ChatOptions;

	fn streamer() -> OllamaStreamer {
		let request = reqwest::Client::new().get("http://localhost");
		let options = ChatOptions::default().with_capture_tool_calls(true);
		let options_set = ChatOptionsSet::default().with_chat_options(Some(&options));
		OllamaStreamer::new(
			WebStream::new_with_delimiter(request, "\n"),
			ModelIden::new(AdapterKind::Ollama, "security-test"),
			options_set,
		)
	}

	fn tool_call(index: usize) -> ToolCall {
		ToolCall {
			call_id: format!("call-{index}"),
			fn_name: "lookup".to_string(),
			fn_arguments: serde_json::json!({}),
			thought_signatures: None,
		}
	}

	#[test]
	fn ordinary_ollama_tool_calls_remain_captured() {
		let mut streamer = streamer();

		streamer
			.capture_tool_calls(&[tool_call(0), tool_call(1)])
			.expect("ordinary tool calls should fit");

		assert_eq!(streamer.captured_data.tool_call_count(), 2);
	}

	#[test]
	fn ollama_tool_call_batches_stop_at_the_shared_limit() {
		let mut streamer = streamer();
		let tool_calls: Vec<_> = (0..=MAX_CAPTURED_TOOL_CALLS).map(tool_call).collect();

		let error = streamer
			.capture_tool_calls(&tool_calls)
			.expect_err("tool call above the configured count should fail");

		assert!(matches!(
			error,
			Error::StreamLimitExceeded {
				resource: "captured tool calls",
				limit: MAX_CAPTURED_TOOL_CALLS
			}
		));
		assert_eq!(streamer.captured_data.tool_call_count(), MAX_CAPTURED_TOOL_CALLS);
	}
}

impl futures::Stream for OllamaStreamer {
	type Item = Result<InterStreamEvent>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		if self.done {
			return Poll::Ready(None);
		}

		while let Poll::Ready(event) = Pin::new(&mut self.inner).poll_next(cx) {
			match event {
				Some(Ok(data_str)) => {
					// Ollama returns ndjson, so each line or chunk is a full JSON object.
					for line in data_str.lines() {
						if line.trim().is_empty() {
							continue;
						}

						let mut data: Value = match serde_json::from_str(line) {
							Ok(val) => val,
							Err(serde_error) => {
								return Poll::Ready(Some(Err(Error::StreamParse {
									model_iden: self.options.model_iden.clone(),
									serde_error,
								})));
							}
						};

						// -- Handle Reasoning Content Chunk
						// Ollama API doc mentions `thinking` field in message object.
						// Some models (like DeepSeek) might also use `reasoning_content`.
						let reasoning = data
							.x_take::<String>("/message/thinking")
							.or_else(|_| data.x_take::<String>("/message/reasoning_content"));

						if let Ok(reasoning) = reasoning {
							// Note: Ollama may return reasoning in chunks, so we check if it's non-empty and return it as a reasoning chunk.
							if !reasoning.is_empty() {
								// Add to the captured_reasoning_content if chat options say so
								if self.options.capture_reasoning_content {
									self.captured_data.append_reasoning_content(&reasoning)?;
								}
								return Poll::Ready(Some(Ok(InterStreamEvent::ReasoningChunk(reasoning))));
							}
						}

						// -- Handle Text Chunk
						if let Ok(content) = data.x_take::<String>("/message/content") {
							// Note: Ollama may return content in chunks, so we check if it's non-empty and return it as a content chunk.
							if !content.is_empty() {
								// Add to the captured_content if chat options say so
								if self.options.capture_content {
									self.captured_data.append_content(&content)?;
								}
								return Poll::Ready(Some(Ok(InterStreamEvent::Chunk(content))));
							}
						}

						// -- Handle Tool Calls Chunk
						if let Ok(tool_calls_value) = data.x_take::<Vec<Value>>("/message/tool_calls") {
							let mut tcs = Vec::new();
							for mut tc_val in tool_calls_value {
								let fn_name: String = tc_val.x_take("/function/name")?;
								let fn_arguments: Value = tc_val.x_take("/function/arguments")?;

								// GenAI requires a call_id.
								// Native Ollama API doesn't always provide one. Generate one if missing.
								let call_id = tc_val
									.x_take::<String>("/id")
									.unwrap_or_else(|_| format!("call_{}", &uuid::Uuid::new_v4().to_string()[..8]));

								let tc = ToolCall {
									call_id,
									fn_name,
									fn_arguments,
									thought_signatures: None,
								};
								tcs.push(tc);
							}

							if !tcs.is_empty() {
								self.capture_tool_calls(&tcs)?;
								// Return the tool call as a chunk
								if let Some(tc) = tcs.into_iter().next() {
									return Poll::Ready(Some(Ok(InterStreamEvent::ToolCallChunk(tc))));
								}
							}
						}

						// -- Handle Message Stop / Done
						let done = data.x_get::<bool>("/done").unwrap_or(false);
						if done {
							self.done = true;

							// Capture done_reason (e.g., "stop", "length")
							self.captured_data.stop_reason = data.x_take::<String>("done_reason").ok();

							if self.options.capture_usage {
								let prompt_tokens = data.x_get::<i32>("/prompt_eval_count").ok();
								let completion_tokens = data.x_get::<i32>("/eval_count").ok();
								let total_tokens = match (prompt_tokens, completion_tokens) {
									(Some(p), Some(c)) => Some(p + c),
									_ => None,
								};

								self.captured_data.usage = Some(Usage {
									prompt_tokens,
									completion_tokens,
									total_tokens,
									..Default::default()
								});
							}

							let inter_stream_end = InterStreamEnd {
								captured_usage: self.captured_data.usage.take(),
								captured_stop_reason: self.captured_data.stop_reason.take().map(StopReason::from),
								captured_text_content: self.captured_data.take_content(),
								captured_reasoning_content: self.captured_data.take_reasoning_content(),
								captured_tool_calls: self.captured_data.take_tool_calls(),
								captured_thought_signatures: None,
								captured_response_id: None,
							};

							return Poll::Ready(Some(Ok(InterStreamEvent::End(inter_stream_end))));
						}
					}
				}
				Some(Err(err)) => {
					return Poll::Ready(Some(Err(Error::WebStream {
						model_iden: self.options.model_iden.clone(),
						cause: err.to_string(),
						error: err,
					})));
				}
				None => {
					if !self.done {
						self.done = true;
						let inter_stream_end = InterStreamEnd {
							captured_usage: self.captured_data.usage.take(),
							captured_stop_reason: self.captured_data.stop_reason.take().map(StopReason::from),
							captured_text_content: self.captured_data.take_content(),
							captured_reasoning_content: self.captured_data.take_reasoning_content(),
							captured_tool_calls: self.captured_data.take_tool_calls(),
							captured_thought_signatures: None,
							captured_response_id: None,
						};
						return Poll::Ready(Some(Ok(InterStreamEvent::End(inter_stream_end))));
					}
					return Poll::Ready(None);
				}
			}
		}

		Poll::Pending
	}
}
