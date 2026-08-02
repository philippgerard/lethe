use crate::adapter::adapters::support::{MAX_CAPTURED_TOOL_CALLS, StreamerCapturedData, StreamerOptions};
use crate::adapter::inter_stream::{InterStreamEnd, InterStreamEvent};
use crate::adapter::openai_resp::resp_types::RespResponse;
use crate::chat::{ChatOptionsSet, StopReason, ToolCall};
use crate::webc::{Event, EventSourceStream};
use crate::{Error, ModelIden, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use value_ext::JsonValueExt;

pub struct OpenAIRespStreamer {
	inner: EventSourceStream,
	options: StreamerOptions,

	// -- Set by the poll_next
	/// Flag to prevent polling the EventSource after a MessageStop event
	done: bool,
	captured_data: StreamerCapturedData,

	in_progress_tool_calls: BTreeMap<usize, ToolCall>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
enum RespStreamEvent {
	#[serde(rename = "response.created")]
	ResponseCreated {
		#[serde(default)]
		_response: Value,
	},

	#[serde(rename = "response.output_item.added")]
	OutputItemAdded { output_index: usize, item: Value },

	#[serde(rename = "response.output_item.done")]
	OutputItemDone {
		#[serde(default)]
		_output_index: usize,
		item: Value,
	},

	#[serde(rename = "response.content_part.added")]
	ContentPartAdded {
		#[serde(default)]
		_output_index: usize,
		#[serde(default)]
		_content_index: usize,
		#[serde(default)]
		_part: Value,
	},

	#[serde(rename = "response.output_text.delta")]
	OutputTextDelta {
		#[serde(default)]
		_output_index: usize,
		#[serde(default)]
		_content_index: usize,
		delta: String,
	},

	#[serde(rename = "response.reasoning_text.delta")]
	ReasoningTextDelta {
		#[serde(default)]
		_output_index: usize,
		#[serde(default)]
		_content_index: usize,
		delta: String,
	},

	// Responses API emits distilled reasoning *summaries* under a
	// separate event family when the request opts into
	// `reasoning.summary = "detailed"`. These are not identical to
	// the raw reasoning-text stream; they're a provider-side summary
	// of the reasoning. Treat them the same way at the adapter layer
	// — append into `captured_data.reasoning_content` — so callers
	// get a single normalized stream regardless of which family the
	// provider chose to emit. Without this handler the summary
	// events fell through to `Unknown` and the reasoning_content
	// field came back empty despite a correct request.
	#[serde(rename = "response.reasoning_summary_text.delta")]
	ReasoningSummaryTextDelta {
		#[serde(default)]
		_output_index: usize,
		#[serde(default)]
		_summary_index: usize,
		delta: String,
	},

	#[serde(rename = "response.function_call_arguments.delta")]
	FunctionCallArgumentsDelta {
		#[serde(default)]
		output_index: usize,
		delta: String,
	},

	#[serde(rename = "response.completed")]
	ResponseCompleted { response: RespResponse },

	#[serde(rename = "response.failed")]
	ResponseFailed { response: RespResponse },

	#[serde(rename = "response.incomplete")]
	ResponseIncomplete { response: RespResponse },

	#[serde(other)]
	Unknown,
}

impl OpenAIRespStreamer {
	pub fn new(inner: EventSourceStream, model_iden: ModelIden, options_set: ChatOptionsSet<'_, '_>) -> Self {
		Self {
			inner,
			done: false,
			options: StreamerOptions::new(model_iden, options_set),
			captured_data: Default::default(),
			in_progress_tool_calls: BTreeMap::new(),
		}
	}

	fn start_tool_call(&mut self, output_index: usize, call_id: String, fn_name: String) -> Result<()> {
		if output_index >= MAX_CAPTURED_TOOL_CALLS {
			return Err(Error::InvalidToolCallIndex {
				index: output_index,
				limit: MAX_CAPTURED_TOOL_CALLS,
			});
		}
		self.captured_data.record_capture(call_id.len() + fn_name.len())?;
		self.in_progress_tool_calls.insert(
			output_index,
			ToolCall {
				call_id,
				fn_name,
				fn_arguments: Value::String(String::new()),
				thought_signatures: None,
			},
		);
		Ok(())
	}

	fn append_tool_arguments(&mut self, output_index: usize, delta: &str) -> Result<Option<ToolCall>> {
		if !self.in_progress_tool_calls.contains_key(&output_index) {
			return Ok(None);
		}
		self.captured_data.record_capture(delta.len())?;
		let tool_call = self
			.in_progress_tool_calls
			.get_mut(&output_index)
			.expect("tool-call presence checked above");
		if let Value::String(arguments) = &mut tool_call.fn_arguments {
			arguments.push_str(delta);
		}
		Ok(Some(tool_call.clone()))
	}
}

#[cfg(test)]
mod security_tests {
	use super::*;
	use crate::adapter::AdapterKind;
	use crate::adapter::adapters::support::MAX_CAPTURED_STREAM_BYTES;
	use crate::chat::ChatOptions;

	fn streamer() -> OpenAIRespStreamer {
		let request = reqwest::Client::new().get("http://localhost");
		let options = ChatOptions::default().with_capture_tool_calls(true);
		let options_set = ChatOptionsSet::default().with_chat_options(Some(&options));
		OpenAIRespStreamer::new(
			EventSourceStream::new(request),
			ModelIden::new(AdapterKind::OpenAIResp, "security-test"),
			options_set,
		)
	}

	#[test]
	fn tool_argument_fragments_preserve_responses_semantics() {
		let mut streamer = streamer();
		streamer
			.start_tool_call(0, "call-0".to_string(), "lookup".to_string())
			.expect("ordinary tool call should start");
		streamer
			.append_tool_arguments(0, "{\"id\":")
			.expect("first fragment should fit");
		let tool_call = streamer
			.append_tool_arguments(0, "1}")
			.expect("second fragment should fit")
			.expect("tool call should exist");

		assert_eq!(tool_call.fn_arguments, Value::String("{\"id\":1}".to_string()));
	}

	#[test]
	fn tool_call_output_index_is_bounded_before_map_growth() {
		let mut streamer = streamer();

		let error = streamer
			.start_tool_call(MAX_CAPTURED_TOOL_CALLS, "call".to_string(), "lookup".to_string())
			.expect_err("index at the exclusive limit should fail");

		assert!(matches!(
			error,
			Error::InvalidToolCallIndex {
				index: MAX_CAPTURED_TOOL_CALLS,
				limit: MAX_CAPTURED_TOOL_CALLS
			}
		));
		assert!(streamer.in_progress_tool_calls.is_empty());
	}

	#[test]
	fn sparse_in_range_responses_output_index_remains_supported() {
		let mut streamer = streamer();

		streamer
			.start_tool_call(MAX_CAPTURED_TOOL_CALLS - 1, "call".to_string(), "lookup".to_string())
			.expect("Responses output indexes need not be dense");

		assert_eq!(streamer.in_progress_tool_calls.len(), 1);
	}

	#[test]
	fn tool_argument_fragments_share_the_capture_byte_budget() {
		let mut streamer = streamer();
		streamer
			.start_tool_call(0, String::new(), String::new())
			.expect("ordinary tool call should start");
		streamer
			.captured_data
			.record_capture(MAX_CAPTURED_STREAM_BYTES)
			.expect("exact byte limit should fit");

		let error = streamer
			.append_tool_arguments(0, "x")
			.expect_err("next argument byte should exceed the shared limit");

		assert!(matches!(
			error,
			Error::StreamLimitExceeded {
				resource: "capture bytes",
				limit: MAX_CAPTURED_STREAM_BYTES
			}
		));
		assert_eq!(
			streamer.in_progress_tool_calls[&0].fn_arguments,
			Value::String(String::new())
		);
	}

	#[test]
	fn responses_capture_channels_share_one_byte_budget() {
		let mut streamer = streamer();
		streamer
			.captured_data
			.record_capture(MAX_CAPTURED_STREAM_BYTES - 2)
			.expect("bytes below the limit should fit");
		streamer
			.captured_data
			.append_content("a")
			.expect("content byte at the boundary should fit");
		streamer
			.captured_data
			.append_reasoning_content("b")
			.expect("reasoning byte at the boundary should fit");

		let error = streamer
			.captured_data
			.push_thought_signature("c".to_string())
			.expect_err("signature must share the exhausted capture budget");

		assert!(matches!(
			error,
			Error::StreamLimitExceeded {
				resource: "capture bytes",
				limit: MAX_CAPTURED_STREAM_BYTES
			}
		));
		assert_eq!(streamer.captured_data.take_content().as_deref(), Some("a"));
		assert_eq!(streamer.captured_data.take_reasoning_content().as_deref(), Some("b"));
		assert!(!streamer.captured_data.has_thought_signatures());
	}
}

impl futures::Stream for OpenAIRespStreamer {
	type Item = Result<InterStreamEvent>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		if self.done {
			return Poll::Ready(None);
		}

		while let Poll::Ready(event) = Pin::new(&mut self.inner).poll_next(cx) {
			match event {
				Some(Ok(Event::Open)) => return Poll::Ready(Some(Ok(InterStreamEvent::Start))),
				Some(Ok(Event::Message(message))) => {
					let stream_event: RespStreamEvent = match serde_json::from_str(&message.data) {
						Ok(stream_event) => stream_event,
						Err(serde_error) => {
							// If we are in debug, we might want to know about this
							tracing::warn!(
								"OpenAIRespStreamer - fail to parse event (skipping). Cause: {serde_error}. Data: {}",
								message.data
							);
							continue;
						}
					};

					match stream_event {
						RespStreamEvent::ResponseCreated { .. } => {
							// For now, we don't need to do anything with the response object here
							continue;
						}

						RespStreamEvent::OutputItemAdded { output_index, item } => {
							if item.x_get_str("type").ok() == Some("function_call") {
								let call_id = item.x_get_str("call_id").unwrap_or_default().to_string();
								let fn_name = item.x_get_str("name").unwrap_or_default().to_string();
								self.start_tool_call(output_index, call_id, fn_name)?;
							}
							continue;
						}

						RespStreamEvent::OutputItemDone { item, .. } => {
							// Capture encrypted reasoning blobs from `type: "reasoning"`
							// items as they finalise. Some backends (tasfn-class proxies)
							// don't include `response.output` in the terminal
							// `response.completed` event — they emit reasoning items only
							// via this stream of `output_item.done` events. Reading them
							// here keeps the prefix cache round-trip working regardless
							// of whether the backend echoes `output` at the end.
							if self.options.capture_reasoning_content
								&& item.x_get_str("type").ok() == Some("reasoning")
								&& let Ok(encrypted) = item.x_get_str("encrypted_content")
								&& !encrypted.is_empty()
							{
								self.captured_data.push_thought_signature(encrypted.to_string())?;
							}
							continue;
						}

						RespStreamEvent::ContentPartAdded { .. } => {
							// We can ignore this as deltas will follow
							continue;
						}

						RespStreamEvent::OutputTextDelta { delta, .. } => {
							if self.options.capture_content {
								self.captured_data.append_content(&delta)?;
							}
							return Poll::Ready(Some(Ok(InterStreamEvent::Chunk(delta))));
						}

						RespStreamEvent::ReasoningTextDelta { delta, .. } => {
							if self.options.capture_reasoning_content {
								self.captured_data.append_reasoning_content(&delta)?;
							}
							return Poll::Ready(Some(Ok(InterStreamEvent::ReasoningChunk(delta))));
						}

						RespStreamEvent::ReasoningSummaryTextDelta { delta, .. } => {
							if self.options.capture_reasoning_content {
								self.captured_data.append_reasoning_content(&delta)?;
							}
							return Poll::Ready(Some(Ok(InterStreamEvent::ReasoningChunk(delta))));
						}

						RespStreamEvent::FunctionCallArgumentsDelta { output_index, delta } => {
							if let Some(tool_call_to_send) = self.append_tool_arguments(output_index, &delta)? {
								return Poll::Ready(Some(Ok(InterStreamEvent::ToolCallChunk(tool_call_to_send))));
							}
							continue;
						}

						RespStreamEvent::ResponseCompleted { response } => {
							self.done = true;
							self.captured_data.stop_reason = Some(response.status.clone());

							if self.options.capture_usage {
								self.captured_data.usage = response.usage.map(Into::into);
							}

							let had_incremental_tool_calls = !self.in_progress_tool_calls.is_empty();
							let mut tool_calls = Vec::new();
							for (_, mut tc) in std::mem::take(&mut self.in_progress_tool_calls) {
								// Parse arguments if they are strings
								if let Some(args_str) = tc.fn_arguments.as_str()
									&& let Ok(args_val) = serde_json::from_str(args_str)
								{
									tc.fn_arguments = args_val;
								}
								tool_calls.push(tc);
							}

							// Fallback: if no tool calls were captured incrementally
							// (e.g., the server sent only response.completed without
							// preceding OutputItemAdded / FunctionCallArgumentsDelta
							// events), extract them from the response.output payload.
							if tool_calls.is_empty() {
								for item in &response.output {
									if item.x_get_str("type").ok() == Some("function_call") {
										let call_id = item.x_get_str("call_id").unwrap_or_default().to_string();
										let fn_name = item.x_get_str("name").unwrap_or_default().to_string();
										let args_str = item.x_get_str("arguments").unwrap_or_default();
										let fn_arguments: Value = serde_json::from_str(args_str)
											.unwrap_or_else(|_| Value::String(args_str.to_string()));

										tool_calls.push(ToolCall {
											call_id,
											fn_name,
											fn_arguments,
											thought_signatures: None,
										});
									}
								}
							}

							if self.options.capture_tool_calls {
								for tool_call in tool_calls {
									if had_incremental_tool_calls {
										self.captured_data.push_accounted_tool_call(tool_call)?;
									} else {
										self.captured_data.push_tool_call(tool_call)?;
									}
								}
							}

							// Extract encrypted reasoning content from output items
							// (OpenAI equivalent of Gemini thought signatures).
							// Only used as a fallback — `output_item.done` is the
							// primary source. Some backends don't echo `output` in
							// `response.completed` (it comes back empty), but for
							// backends that do, this picks up anything missed.
							if self.options.capture_reasoning_content && !self.captured_data.has_thought_signatures() {
								for item in &response.output {
									if item.x_get_str("type").ok() == Some("reasoning")
										&& let Ok(encrypted) = item.x_get_str("encrypted_content")
									{
										self.captured_data.push_thought_signature(encrypted.to_string())?;
									}
								}
							}

							let inter_stream_end = InterStreamEnd {
								captured_usage: self.captured_data.usage.take(),
								captured_stop_reason: self.captured_data.stop_reason.take().map(StopReason::from),
								captured_text_content: self.captured_data.take_content(),
								captured_reasoning_content: self.captured_data.take_reasoning_content(),
								captured_tool_calls: self.captured_data.take_tool_calls(),
								captured_thought_signatures: self.captured_data.take_thought_signatures(),
								captured_response_id: Some(response.id),
							};

							return Poll::Ready(Some(Ok(InterStreamEvent::End(inter_stream_end))));
						}

						RespStreamEvent::ResponseFailed { response } => {
							self.done = true;
							let error_msg = response
								.error
								.as_ref()
								.and_then(|e| e.x_get_str("message").ok())
								.unwrap_or("OpenAI Response Failed");

							return Poll::Ready(Some(Err(Error::StreamParse {
								model_iden: self.options.model_iden.clone(),
								serde_error: serde::de::Error::custom(error_msg),
							})));
						}

						RespStreamEvent::ResponseIncomplete { response } => {
							self.done = true;
							self.captured_data.stop_reason = Some(response.status.clone());
							// For incomplete, we might still want to return what we have?
							// But for now, let's treat it as a successful end but with whatever we captured.
							let resp_id = response.id.clone();
							let inter_stream_end = InterStreamEnd {
								captured_usage: response.usage.map(Into::into),
								captured_stop_reason: self.captured_data.stop_reason.take().map(StopReason::from),
								captured_text_content: self.captured_data.take_content(),
								captured_reasoning_content: self.captured_data.take_reasoning_content(),
								captured_tool_calls: self.captured_data.take_tool_calls(),
								captured_thought_signatures: None,
								captured_response_id: Some(resp_id),
							};

							return Poll::Ready(Some(Ok(InterStreamEvent::End(inter_stream_end))));
						}

						RespStreamEvent::Unknown => {
							continue;
						}
					}
				}
				Some(Err(err)) => {
					tracing::error!("Error: {}", err);
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
