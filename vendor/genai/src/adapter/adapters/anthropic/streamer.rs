use crate::adapter::adapters::support::{MAX_CAPTURED_STREAM_BYTES, StreamerCapturedData, StreamerOptions};
use crate::adapter::anthropic::{anthropic_replay_marker, parse_cache_creation_details};
use crate::adapter::inter_stream::{InterStreamEnd, InterStreamEvent};
use crate::chat::{ChatOptionsSet, PromptTokensDetails, StopReason, ToolCall, Usage};
use crate::webc::{Event, EventSourceStream};
use crate::{Error, ModelIden, Result};
use serde_json::{Map, Value};
use std::pin::Pin;
use std::task::{Context, Poll};
use value_ext::JsonValueExt;

pub struct AnthropicStreamer {
	inner: EventSourceStream,
	options: StreamerOptions,

	// -- Set by the poll_next
	/// Flag to prevent polling the EventSource after a MessageStop event
	done: bool,

	captured_data: StreamerCapturedData,
	in_progress_block: InProgressBlock,
	raw_blocks: Vec<Value>,
	active_block_index: Option<usize>,
	replay_bytes: usize,
}

enum InProgressBlock {
	Text,
	ToolUse { id: String, name: String, input: String },
	Thinking,
}

impl AnthropicStreamer {
	pub fn new(inner: EventSourceStream, model_iden: ModelIden, options_set: ChatOptionsSet<'_, '_>) -> Self {
		Self {
			inner,
			done: false,
			options: StreamerOptions::new(model_iden, options_set),
			captured_data: Default::default(),
			in_progress_block: InProgressBlock::Text,
			raw_blocks: Vec::new(),
			active_block_index: None,
			replay_bytes: 0,
		}
	}

	fn record_replay_bytes(&mut self, bytes: usize) -> Result<()> {
		let next = self.replay_bytes.checked_add(bytes).ok_or(Error::StreamLimitExceeded {
			resource: "Anthropic assistant replay content",
			limit: MAX_CAPTURED_STREAM_BYTES,
		})?;
		if next > MAX_CAPTURED_STREAM_BYTES {
			return Err(Error::StreamLimitExceeded {
				resource: "Anthropic assistant replay content",
				limit: MAX_CAPTURED_STREAM_BYTES,
			});
		}
		self.replay_bytes = next;
		Ok(())
	}

	fn append_raw_field(&mut self, field: &str, fragment: &str) -> Result<()> {
		self.record_replay_bytes(fragment.len())?;
		if let Some(block) = self.active_block_index.and_then(|index| self.raw_blocks.get_mut(index))
			&& let Some(object) = block.as_object_mut()
		{
			let entry = object.entry(field).or_insert_with(|| Value::String(String::new()));
			if let Value::String(text) = entry {
				text.push_str(fragment);
			}
		}
		Ok(())
	}
}

impl futures::Stream for AnthropicStreamer {
	type Item = Result<InterStreamEvent>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		if self.done {
			return Poll::Ready(None);
		}

		while let Poll::Ready(event) = Pin::new(&mut self.inner).poll_next(cx) {
			// NOTE: At this point, we capture more events than needed for genai::StreamItem, but it serves as documentation.
			match event {
				Some(Ok(Event::Open)) => return Poll::Ready(Some(Ok(InterStreamEvent::Start))),
				Some(Ok(Event::Message(message))) => {
					let message_type = message.event.as_str();

					match message_type {
						"message_start" => {
							self.capture_usage(message_type, &message.data)?;
							continue;
						}
						"message_delta" => {
							self.capture_usage(message_type, &message.data)?;
							// Capture stop_reason from delta (e.g., "end_turn", "max_tokens", "tool_use")
							if let Ok(data) = self.parse_message_data(&message.data)
								&& let Ok(reason) = data.x_get::<String>("/delta/stop_reason")
							{
								self.captured_data.stop_reason = Some(reason);
							}
							continue;
						}
						"content_block_start" => {
							if self.active_block_index.is_some() {
								return Poll::Ready(Some(Err(Error::InvalidJsonResponseElement {
									info: "Anthropic content block started before previous block closed",
								})));
							}
							let mut data: Value =
								serde_json::from_str(&message.data).map_err(|serde_error| Error::StreamParse {
									model_iden: self.options.model_iden.clone(),
									serde_error,
								})?;
							let index: usize = data.x_get("/index")?;
							if index >= 512 {
								return Poll::Ready(Some(Err(Error::StreamLimitExceeded {
									resource: "Anthropic assistant content blocks",
									limit: 512,
								})));
							}
							let block: Value = data.x_get("/content_block")?;
							self.record_replay_bytes(block.to_string().len())?;
							self.raw_blocks.resize(index + 1, Value::Null);
							self.raw_blocks[index] = block;
							self.active_block_index = Some(index);

							match data.x_get_str("/content_block/type") {
								Ok("text") => self.in_progress_block = InProgressBlock::Text,
								Ok("thinking") => self.in_progress_block = InProgressBlock::Thinking,
								Ok("tool_use") => {
									let id: String = data.x_take("/content_block/id")?;
									let name: String = data.x_take("/content_block/name")?;
									self.captured_data.record_capture(id.len() + name.len())?;

									// Emit an initial ToolCallChunk with name and empty args,
									// matching OpenAI's incremental streaming behaviour.
									let tc = ToolCall {
										call_id: id.clone(),
										fn_name: name.clone(),
										fn_arguments: Value::String(String::new()),
										thought_signatures: None,
									};
									self.in_progress_block = InProgressBlock::ToolUse {
										id,
										name,
										input: String::new(),
									};

									return Poll::Ready(Some(Ok(InterStreamEvent::ToolCallChunk(tc))));
								}
								Ok(txt) => {
									tracing::warn!("unhandled content type: {txt}");
								}
								Err(e) => {
									tracing::error!("{e:?}");
								}
							}

							continue;
						}
						"content_block_delta" => {
							let mut data: Value =
								serde_json::from_str(&message.data).map_err(|serde_error| Error::StreamParse {
									model_iden: self.options.model_iden.clone(),
									serde_error,
								})?;
							let index: usize = data.x_get("/index")?;
							if self.active_block_index != Some(index) {
								return Poll::Ready(Some(Err(Error::InvalidJsonResponseElement {
									info: "Anthropic content delta has invalid block index",
								})));
							}
							match data.x_get_str("/delta/type")? {
								"text_delta" => self.append_raw_field("text", data.x_get_str("/delta/text")?)?,
								"thinking_delta" => {
									self.append_raw_field("thinking", data.x_get_str("/delta/thinking")?)?
								}
								"signature_delta" => {
									self.append_raw_field("signature", data.x_get_str("/delta/signature")?)?
								}
								"input_json_delta" => {
									self.record_replay_bytes(data.x_get_str("/delta/partial_json")?.len())?
								}
								_ => {}
							}

							if matches!(self.in_progress_block, InProgressBlock::ToolUse { .. }) {
								let partial_json_len = data.x_get_str("/delta/partial_json")?.len();
								self.captured_data.record_capture(partial_json_len)?;
							}

							match &mut self.in_progress_block {
								InProgressBlock::Text => {
									let content: String = data.x_take("/delta/text")?;

									// Add to the captured_content if chat options say so
									if self.options.capture_content {
										self.captured_data.append_content(&content)?;
									}

									return Poll::Ready(Some(Ok(InterStreamEvent::Chunk(content))));
								}
								InProgressBlock::ToolUse { id, name, input } => {
									let partial = data.x_get_str("/delta/partial_json")?;
									input.push_str(partial);

									// Emit incremental ToolCallChunk with accumulated args
									// (as Value::String, same convention as OpenAI adapter).
									let tc = ToolCall {
										call_id: id.clone(),
										fn_name: name.clone(),
										fn_arguments: Value::String(input.clone()),
										thought_signatures: None,
									};

									return Poll::Ready(Some(Ok(InterStreamEvent::ToolCallChunk(tc))));
								}
								InProgressBlock::Thinking => {
									if let Ok(thinking) = data.x_take::<String>("/delta/thinking") {
										// Add to the captured_thinking if chat options say so
										if self.options.capture_reasoning_content {
											self.captured_data.append_reasoning_content(&thinking)?;
										}

										return Poll::Ready(Some(Ok(InterStreamEvent::ReasoningChunk(thinking))));
									} else if let Ok(signature) = data.x_take::<String>("/delta/signature") {
										return Poll::Ready(Some(Ok(InterStreamEvent::ThoughtSignatureChunk(
											signature,
										))));
									} else {
										// If it is thinking but no thinking or signature field, we log and skip.
										tracing::warn!(
											"content_block_delta for thinking block but no thinking or signature found: {data:?}"
										);
										continue;
									}
								}
							}
						}
						"content_block_stop" => {
							let data: Value = serde_json::from_str(&message.data)?;
							let index: usize = data.x_get("/index")?;
							if self.active_block_index != Some(index) {
								return Poll::Ready(Some(Err(Error::InvalidJsonResponseElement {
									info: "Anthropic content block stop has invalid index",
								})));
							}
							let block_index = self.active_block_index.take();
							match std::mem::replace(&mut self.in_progress_block, InProgressBlock::Text) {
								InProgressBlock::ToolUse { id, name, input } if self.options.capture_tool_calls => {
									// ToolCallChunks were already emitted incrementally
									// during content_block_start and content_block_delta.
									// Here we only finalize capture with parsed arguments.
									let fn_arguments = if input.is_empty() {
										Value::Object(Map::new())
									} else {
										serde_json::from_str(&input)?
									};
									if let Some(block) = block_index.and_then(|index| self.raw_blocks.get_mut(index))
										&& let Some(object) = block.as_object_mut()
									{
										object.insert("input".to_string(), fn_arguments.clone());
									}

									let tc = ToolCall {
										call_id: id,
										fn_name: name,
										fn_arguments,
										thought_signatures: None,
									};

									self.captured_data.push_tool_call(tc)?;
								}
								_ => {
									// no-op for remaining block types
								}
							}

							continue;
						}
						// -- END MESSAGE
						"message_stop" => {
							if self.active_block_index.is_some() {
								self.done = true;
								return Poll::Ready(Some(Err(Error::InvalidJsonResponseElement {
									info: "Anthropic message stopped with an open content block",
								})));
							}
							// Ensure we do not poll the EventSource anymore on the next poll.
							// NOTE: This way, the last MessageStop event is still sent,
							//       but then, on the next poll, it will be stopped.
							self.done = true;

							// Capture the usage
							let captured_usage = if self.options.capture_usage {
								self.captured_data.usage.take().map(|mut usage| {
									// Compute the total if any of input/output are not null
									if usage.prompt_tokens.is_some() || usage.completion_tokens.is_some() {
										usage.total_tokens = Some(
											usage.prompt_tokens.unwrap_or(0) + usage.completion_tokens.unwrap_or(0),
										);
									}
									usage
								})
							} else {
								None
							};

							let mut captured_tool_calls = self.captured_data.take_tool_calls();
							if let Some(calls) = captured_tool_calls.as_mut()
								&& let Some(first) = calls.first_mut()
								&& let Some(marker) =
									anthropic_replay_marker(&self.options.model_iden.model_name, &self.raw_blocks)?
							{
								first.thought_signatures = Some(vec![marker]);
							}
							let inter_stream_end = InterStreamEnd {
								captured_usage,
								captured_stop_reason: self.captured_data.stop_reason.take().map(StopReason::from),
								captured_text_content: self.captured_data.take_content(),
								captured_reasoning_content: self.captured_data.take_reasoning_content(),
								captured_tool_calls,
								captured_thought_signatures: None,
								captured_response_id: None,
							};

							// TODO: Need to capture the data as needed
							return Poll::Ready(Some(Ok(InterStreamEvent::End(inter_stream_end))));
						}

						"error" => {
							self.done = true;
							let body =
								serde_json::from_str(&message.data).unwrap_or_else(|_| Value::String(message.data));
							return Poll::Ready(Some(Err(Error::ChatResponse {
								model_iden: self.options.model_iden.clone(),
								body,
							})));
						}
						"ping" => continue, // Loop to the next event
						other => tracing::warn!("UNKNOWN MESSAGE TYPE: {other}"),
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
					self.done = true;
					return Poll::Ready(Some(Err(Error::Internal(
						"Anthropic stream ended before message_stop".to_string(),
					))));
				}
			}
		}
		Poll::Pending
	}
}

// Support
impl AnthropicStreamer {
	fn capture_usage(&mut self, message_type: &str, message_data: &str) -> Result<()> {
		if self.options.capture_usage {
			let data = self.parse_message_data(message_data)?;
			// TODO: Might want to exit early if usage is not found

			let (input_path, output_path) = if message_type == "message_start" {
				("/message/usage/input_tokens", "/message/usage/output_tokens")
			} else if message_type == "message_delta" {
				("/usage/input_tokens", "/usage/output_tokens")
			} else {
				// TODO: Use tracing
				tracing::debug!(
					"TRACING DEBUG - Anthropic message type not supported for input/output tokens: {message_type}"
				);
				return Ok(()); // For now permissive
			};

			// -- Capture/Add the eventual input_tokens
			// NOTE: Permissive on this one; if an error occurs, treat it as nonexistent (for now)
			if let Ok(input_tokens) = data.x_get::<i32>(input_path) {
				let val = self
					.captured_data
					.usage
					.get_or_insert(Usage::default())
					.prompt_tokens
					.get_or_insert(0);
				*val += input_tokens;
			}

			if let Ok(output_tokens) = data.x_get::<i32>(output_path) {
				let val = self
					.captured_data
					.usage
					.get_or_insert(Usage::default())
					.completion_tokens
					.get_or_insert(0);
				*val += output_tokens;
			}

			// -- Capture cache tokens (only present in message_start)
			// NOTE: Anthropic's input_tokens does NOT include cached tokens, so we must add them.
			// See also: AnthropicAdapter::into_usage() for non-streaming equivalent.
			if message_type == "message_start" {
				let cache_creation: i32 = data.x_get("/message/usage/cache_creation_input_tokens").unwrap_or(0);
				let cache_read: i32 = data.x_get("/message/usage/cache_read_input_tokens").unwrap_or(0);

				// Parse cache_creation breakdown if present (TTL-specific breakdown)
				// Use x_get with JSON pointer to navigate to /message/usage/cache_creation
				let cache_creation_details = data
					.x_get::<Value>("/message/usage/cache_creation")
					.ok()
					.as_ref()
					.and_then(parse_cache_creation_details);

				if cache_creation > 0 || cache_read > 0 || cache_creation_details.is_some() {
					let usage = self.captured_data.usage.get_or_insert(Usage::default());

					// Add cache tokens to prompt_tokens (same as into_usage does)
					if let Some(ref mut pt) = usage.prompt_tokens {
						*pt += cache_creation + cache_read;
					}

					// Set prompt_tokens_details (match into_usage behavior: always Some(value))
					usage.prompt_tokens_details = Some(PromptTokensDetails {
						cache_creation_tokens: Some(cache_creation),
						cache_creation_details,
						cached_tokens: Some(cache_read),
						audio_tokens: None,
					});
				}
			}
		}

		Ok(())
	}

	/// Simple wrapper for now, with the corresponding map_err.
	/// Might have more logic later.
	fn parse_message_data(&self, payload: &str) -> Result<Value> {
		serde_json::from_str(payload).map_err(|serde_error| Error::StreamParse {
			model_iden: self.options.model_iden.clone(),
			serde_error,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures::StreamExt;
	use std::io::{Read, Write};
	use std::net::TcpListener;

	fn local_sse(body: &'static str) -> (EventSourceStream, std::thread::JoinHandle<()>) {
		let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener should bind");
		let address = listener.local_addr().expect("listener should have an address");
		let server = std::thread::spawn(move || {
			let (mut socket, _) = listener.accept().expect("test client should connect");
			let mut request = [0_u8; 4096];
			let _ = socket.read(&mut request);
			let headers = format!(
				"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
				body.len()
			);
			socket.write_all(headers.as_bytes()).unwrap();
			socket.write_all(body.as_bytes()).unwrap();
		});
		let client = reqwest::Client::builder().no_proxy().build().unwrap();
		(
			EventSourceStream::new(client.get(format!("http://{address}/messages"))),
			server,
		)
	}

	async fn stream_error(body: &'static str) -> Error {
		let (inner, server) = local_sse(body);
		let stream = AnthropicStreamer::new(
			inner,
			ModelIden::new(crate::adapter::AdapterKind::Anthropic, "claude-opus-5-5"),
			ChatOptionsSet::default(),
		);
		let mut stream = Box::pin(stream);
		assert!(matches!(stream.next().await, Some(Ok(InterStreamEvent::Start))));
		let error = stream
			.next()
			.await
			.expect("stream must report an error")
			.expect_err("stream must not succeed");
		server.join().expect("test server should exit");
		error
	}

	#[tokio::test]
	async fn provider_error_frame_fails_the_stream() {
		let error =
			stream_error("event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"overloaded\"}}\n\n").await;
		assert!(matches!(error, Error::ChatResponse { .. }));
	}

	#[tokio::test]
	async fn premature_eof_and_open_block_stop_fail_the_stream() {
		let start = "event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n";
		let eof_error = stream_error(start).await;
		assert!(matches!(eof_error, Error::Internal(_)));
		let stop_error = stream_error("event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\nevent: message_stop\ndata: {}\n\n").await;
		assert!(matches!(stop_error, Error::InvalidJsonResponseElement { .. }));
	}
}
