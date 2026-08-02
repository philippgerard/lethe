use bytes::Bytes;
use futures::stream::TryStreamExt;
use futures::{Future, Stream};
use reqwest::{RequestBuilder, Response};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{BoxError, Error as GenaiError};
use crate::webc::{Error as WebError, MAX_ERROR_BODY_BYTES, response_text_limited};

pub(crate) const MAX_STREAM_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_STREAM_EVENT_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_STREAM_EVENTS: usize = 65_536;
pub(crate) const MAX_STREAM_BUFFER_BYTES: usize = MAX_STREAM_CHUNK_BYTES + MAX_STREAM_EVENT_BYTES;

/// WebStream is a simple web stream implementation that splits the stream messages by a given delimiter.
/// - It is intended to be a pragmatic solution for services that do not adhere to the `text/event-stream` format and content type.
/// - For providers that support the standard `text/event-stream`, `genai` uses the `reqwest-eventsource`/`eventsource-stream` crates.
/// - This stream item is just a `String` and has different stream modes that define the message delimiter strategy (without any event typing).
/// - Each "Event" is just string-based and has only one event type, which is a string.
/// - It is the responsibility of the user of this stream to wrap it into a semantically correct stream of events depending on the domain.
#[allow(clippy::type_complexity)]
pub struct WebStream {
	stream_mode: StreamMode,
	reqwest_builder: Option<RequestBuilder>,
	response_future: Option<Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send>>>,
	bytes_stream: Option<Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send>>>,
	// If a poll was a partial message, then we keep the previous part
	partial_message: Option<String>,
	// If a poll retrieved multiple messages, we keep them to be sent in the next poll
	remaining_messages: Option<VecDeque<String>>,
	// Incomplete trailing UTF-8 bytes from the previous chunk.
	// When a multi-byte character is split across TCP/HTTP chunk boundaries,
	// the trailing bytes are carried over to be prepended to the next chunk.
	utf8_carry: Vec<u8>,
	accepted_events: usize,
}

pub enum StreamMode {
	// This is used for Cohere with a single `\n`
	Delimiter(&'static str),
	// Server-Sent Events: events terminated by `\n\n`, `\r\n\r\n`, or `\r\r`.
	// Per the SSE spec, all line-ending variants are accepted; we normalize CR/LF
	// to LF before splitting so a single `\n\n` delimiter matches all of them.
	Sse,
}

impl WebStream {
	pub fn new_with_delimiter(reqwest_builder: RequestBuilder, message_delimiter: &'static str) -> Self {
		Self {
			stream_mode: StreamMode::Delimiter(message_delimiter),
			reqwest_builder: Some(reqwest_builder),
			response_future: None,
			bytes_stream: None,
			partial_message: None,
			remaining_messages: None,
			utf8_carry: Vec::new(),
			accepted_events: 0,
		}
	}

	pub fn new_with_sse(reqwest_builder: RequestBuilder) -> Self {
		Self {
			stream_mode: StreamMode::Sse,
			reqwest_builder: Some(reqwest_builder),
			response_future: None,
			bytes_stream: None,
			partial_message: None,
			remaining_messages: None,
			utf8_carry: Vec::new(),
			accepted_events: 0,
		}
	}
}

impl Stream for WebStream {
	type Item = Result<String, BoxError>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();

		// -- First, we check if we have any remaining messages to send.
		if let Some(ref mut remaining_messages) = this.remaining_messages
			&& let Some(msg) = remaining_messages.pop_front()
		{
			return Poll::Ready(Some(Ok(msg)));
		}

		// -- Then execute the web poll and processing loop
		loop {
			if let Some(ref mut fut) = this.response_future {
				match Pin::new(fut).poll(cx) {
					Poll::Ready(Ok(response)) => {
						// Check HTTP status before proceeding with the stream
						let status = response.status();
						if !status.is_success() {
							this.response_future = None;
							// For error responses, we need to read the body to get the error message
							// Store a future that reads the body and returns an error
							let error_future = async move {
								let body = match response_text_limited(response, MAX_ERROR_BODY_BYTES).await {
									Ok(body) => body,
									Err(error @ WebError::ResponseBodyTooLarge { .. }) => {
										return Err(Box::new(error) as BoxError);
									}
									Err(error) => format!("Failed to read error body: {error}"),
								};
								Err::<Response, BoxError>(Box::new(GenaiError::HttpError {
									status,
									canonical_reason: status.canonical_reason().unwrap_or("Unknown").to_string(),
									body,
								}))
							};
							this.response_future = Some(Box::pin(error_future));
							continue;
						}
						let bytes_stream = response.bytes_stream().map_err(|e| Box::new(e) as BoxError);
						this.bytes_stream = Some(Box::pin(bytes_stream));
						this.response_future = None;
					}
					Poll::Ready(Err(e)) => {
						this.response_future = None;
						return Poll::Ready(Some(Err(e)));
					}
					Poll::Pending => return Poll::Pending,
				}
			}

			if let Some(ref mut stream) = this.bytes_stream {
				match stream.as_mut().poll_next(cx) {
					Poll::Ready(Some(Ok(bytes))) => {
						if let Err(error) = ensure_stream_chunk_size(this.utf8_carry.len(), bytes.len()) {
							return Poll::Ready(Some(Err(Box::new(error))));
						}

						// -- Incremental UTF-8 decoding: prepend any carried-over
						// bytes from the previous chunk, decode as much valid UTF-8
						// as possible, and carry over any incomplete trailing sequence.
						let mut raw = std::mem::take(&mut this.utf8_carry);
						raw.extend_from_slice(&bytes);

						let valid_up_to = match std::str::from_utf8(&raw) {
							Ok(_) => raw.len(),
							Err(e) => {
								if e.error_len().is_some() {
									// Actual invalid UTF-8 (not just incomplete) — fatal error.
									return Poll::Ready(Some(Err(
										Box::new(String::from_utf8(raw).unwrap_err()) as BoxError
									)));
								}
								e.valid_up_to()
							}
						};

						// Carry over incomplete trailing bytes for the next chunk.
						this.utf8_carry = raw[valid_up_to..].to_vec();

						// We already validated raw[..valid_up_to] is valid UTF-8 above.
						let buff_string = String::from_utf8(raw[..valid_up_to].to_vec()).unwrap();

						// -- Iterate through the parts
						let remaining_event_capacity = MAX_STREAM_EVENTS.saturating_sub(this.accepted_events);
						let buff_response = match this.stream_mode {
							StreamMode::Delimiter(delimiter) => process_buff_string_delimited(
								buff_string,
								&mut this.partial_message,
								delimiter,
								remaining_event_capacity,
							),
							StreamMode::Sse => process_buff_string_sse(
								buff_string,
								&mut this.partial_message,
								remaining_event_capacity,
							),
						};

						let BuffResponse {
							mut first_message,
							next_messages,
							candidate_message,
						} = buff_response?;
						let accepted_now = usize::from(first_message.is_some())
							+ next_messages.as_ref().map(Vec::len).unwrap_or_default();
						if let Err(error) = record_stream_events(&mut this.accepted_events, accepted_now) {
							return Poll::Ready(Some(Err(Box::new(error))));
						}

						// -- Add next_messages as remaining messages if present
						if let Some(next_messages) = next_messages {
							this.remaining_messages.get_or_insert(VecDeque::new()).extend(next_messages);
						}

						// -- If we still have a candidate, it's the partial for the next one
						if let Some(candidate_message) = candidate_message {
							// For now, we will just log this
							if this.partial_message.is_some() {
								tracing::warn!("GENAI - WARNING - partial_message is not none");
							}
							this.partial_message = Some(candidate_message);
						}

						// -- If we have a first message, we have to send it.
						if let Some(first_message) = first_message.take() {
							return Poll::Ready(Some(Ok(first_message)));
						} else {
							continue;
						}
					}
					Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
					Poll::Ready(None) => {
						if let Some(partial) = this.partial_message.take()
							&& !partial.is_empty()
						{
							if let Err(error) = ensure_stream_event_size(partial.len()) {
								return Poll::Ready(Some(Err(Box::new(error))));
							}
							if let Err(error) = record_stream_events(&mut this.accepted_events, 1) {
								return Poll::Ready(Some(Err(Box::new(error))));
							}
							return Poll::Ready(Some(Ok(partial)));
						}
						this.bytes_stream = None;
					}
					Poll::Pending => return Poll::Pending,
				}
			}

			if let Some(reqwest_builder) = this.reqwest_builder.take() {
				let fut = async move { reqwest_builder.send().await.map_err(|e| Box::new(e) as BoxError) };
				this.response_future = Some(Box::pin(fut));
				continue;
			}

			return Poll::Ready(None);
		}
	}
}

#[derive(Debug)]
struct BuffResponse {
	first_message: Option<String>,
	next_messages: Option<Vec<String>>,
	candidate_message: Option<String>,
}

fn stream_limit(resource: &'static str, limit: usize) -> WebError {
	WebError::StreamLimitExceeded { resource, limit }
}

fn ensure_stream_chunk_size(partial_bytes: usize, chunk_bytes: usize) -> Result<(), WebError> {
	let combined = partial_bytes
		.checked_add(chunk_bytes)
		.ok_or_else(|| stream_limit("chunk bytes", MAX_STREAM_CHUNK_BYTES))?;
	if combined > MAX_STREAM_CHUNK_BYTES {
		return Err(stream_limit("chunk bytes", MAX_STREAM_CHUNK_BYTES));
	}
	Ok(())
}

fn ensure_stream_event_size(bytes: usize) -> Result<(), WebError> {
	if bytes > MAX_STREAM_EVENT_BYTES {
		return Err(stream_limit("event bytes", MAX_STREAM_EVENT_BYTES));
	}
	Ok(())
}

fn checked_stream_buffer_size(partial_bytes: usize, chunk_bytes: usize) -> Result<usize, WebError> {
	let combined = partial_bytes
		.checked_add(chunk_bytes)
		.ok_or_else(|| stream_limit("buffer bytes", MAX_STREAM_BUFFER_BYTES))?;
	if combined > MAX_STREAM_BUFFER_BYTES {
		return Err(stream_limit("buffer bytes", MAX_STREAM_BUFFER_BYTES));
	}
	Ok(combined)
}

fn record_stream_events(accepted: &mut usize, additional: usize) -> Result<(), WebError> {
	let next = accepted
		.checked_add(additional)
		.ok_or_else(|| stream_limit("event count", MAX_STREAM_EVENTS))?;
	if next > MAX_STREAM_EVENTS {
		return Err(stream_limit("event count", MAX_STREAM_EVENTS));
	}
	*accepted = next;
	Ok(())
}

fn push_stream_message(messages: &mut Vec<String>, message: &str, remaining_events: usize) -> Result<(), WebError> {
	if messages.len() >= remaining_events {
		return Err(stream_limit("event count", MAX_STREAM_EVENTS));
	}
	messages.push(message.to_string());
	Ok(())
}

fn prepend_partial_message(buff_string: String, partial_message: &mut Option<String>) -> Result<String, WebError> {
	if let Some(mut partial) = partial_message.take() {
		checked_stream_buffer_size(partial.len(), buff_string.len())?;
		partial.push_str(&buff_string);
		Ok(partial)
	} else {
		Ok(buff_string)
	}
}

/// Process a string buffer for SSE mode.
///
/// Per the SSE spec, event boundaries can be `\n\n`, `\r\n\r\n`, or `\r\r`. Browsers
/// normalize all CR/CRLF to LF before dispatching, so we do the same: collapse to LF
/// and reuse the standard `\n\n` delimiter splitter. This keeps cross-chunk partial
/// handling identical to the regular delimited path.
fn process_buff_string_sse(
	buff_string: String,
	partial_message: &mut Option<String>,
	remaining_events: usize,
) -> Result<BuffResponse, crate::webc::Error> {
	ensure_stream_chunk_size(0, buff_string.len())?;
	// Normalize after concatenation with any prior partial — partial may have
	// left a lone `\r` that pairs with the next chunk's leading `\n`.
	let full_string = prepend_partial_message(buff_string, partial_message)?;
	let normalized = full_string.replace("\r\n", "\n").replace('\r', "\n");
	split_delimited(normalized, "\n\n", remaining_events)
}

/// Process a string buffer for the delimited mode (e.g., Cohere)
fn process_buff_string_delimited(
	buff_string: String,
	partial_message: &mut Option<String>,
	delimiter: &str,
	remaining_events: usize,
) -> Result<BuffResponse, crate::webc::Error> {
	ensure_stream_chunk_size(0, buff_string.len())?;
	let full_string = prepend_partial_message(buff_string, partial_message)?;
	split_delimited(full_string, delimiter, remaining_events)
}

fn split_delimited(
	full_string: String,
	delimiter: &str,
	remaining_events: usize,
) -> Result<BuffResponse, crate::webc::Error> {
	let mut messages = Vec::new();
	let mut candidate_message = None;
	let mut parts = full_string.split(delimiter).peekable();
	while let Some(part) = parts.next() {
		ensure_stream_event_size(part.len())?;
		if parts.peek().is_none() {
			candidate_message = Some(part.to_string());
			break;
		}
		// Filter out empty strings from repeated delimiters without allocating for them.
		if !part.is_empty() {
			push_stream_message(&mut messages, part, remaining_events)?;
		}
	}

	let mut messages = messages.into_iter();
	let first_message = messages.next();
	let remaining_messages: Vec<_> = messages.collect();
	let next_messages = (!remaining_messages.is_empty()).then_some(remaining_messages);

	Ok(BuffResponse {
		first_message,
		next_messages,
		candidate_message,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn delimited_parser_preserves_legitimate_messages_and_partial_data() {
		let mut partial = None;

		let response =
			process_buff_string_delimited("one\ntwo\npartial".to_string(), &mut partial, "\n", MAX_STREAM_EVENTS)
				.expect("legitimate stream chunk should parse");

		assert_eq!(response.first_message.as_deref(), Some("one"));
		assert_eq!(response.next_messages, Some(vec!["two".to_string()]));
		assert_eq!(response.candidate_message.as_deref(), Some("partial"));
	}

	#[test]
	fn sse_parser_preserves_upstream_line_ending_variants() {
		let mut partial = None;

		let response = process_buff_string_sse(
			"data: one\r\n\r\ndata: two\r\rpartial".to_string(),
			&mut partial,
			MAX_STREAM_EVENTS,
		)
		.expect("legitimate SSE chunk should parse");

		assert_eq!(response.first_message.as_deref(), Some("data: one"));
		assert_eq!(response.next_messages, Some(vec!["data: two".to_string()]));
		assert_eq!(response.candidate_message.as_deref(), Some("partial"));
	}

	#[test]
	fn delimited_parser_rejects_an_oversized_partial_event() {
		let mut partial = Some("x".repeat(MAX_STREAM_EVENT_BYTES));

		let error = process_buff_string_delimited("y".to_string(), &mut partial, "\n", MAX_STREAM_EVENTS)
			.expect_err("partial event should exceed the limit");

		assert!(matches!(
			error,
			WebError::StreamLimitExceeded {
				resource: "event bytes",
				limit: MAX_STREAM_EVENT_BYTES
			}
		));
	}

	#[test]
	fn sse_parser_rejects_an_oversized_partial_event() {
		let mut partial = Some("x".repeat(MAX_STREAM_EVENT_BYTES));

		let error = process_buff_string_sse("y".to_string(), &mut partial, MAX_STREAM_EVENTS)
			.expect_err("partial SSE event should exceed the limit");

		assert!(matches!(
			error,
			WebError::StreamLimitExceeded {
				resource: "event bytes",
				limit: MAX_STREAM_EVENT_BYTES
			}
		));
	}

	#[test]
	fn stream_event_counter_accepts_the_limit_and_rejects_the_next_event() {
		let mut accepted = MAX_STREAM_EVENTS - 1;
		record_stream_events(&mut accepted, 1).expect("event at the limit should fit");

		let error = record_stream_events(&mut accepted, 1).expect_err("next event should fail");

		assert!(matches!(
			error,
			WebError::StreamLimitExceeded {
				resource: "event count",
				limit: MAX_STREAM_EVENTS
			}
		));
		assert_eq!(accepted, MAX_STREAM_EVENTS);
	}

	#[test]
	fn stream_chunk_limit_rejects_oversized_network_chunks() {
		let error =
			ensure_stream_chunk_size(0, MAX_STREAM_CHUNK_BYTES + 1).expect_err("oversized network chunk should fail");

		assert!(matches!(
			error,
			WebError::StreamLimitExceeded {
				resource: "chunk bytes",
				limit: MAX_STREAM_CHUNK_BYTES
			}
		));
	}

	#[test]
	fn delimited_parser_rejects_event_fanout_before_queue_growth() {
		let mut partial = None;

		let error = process_buff_string_delimited("one\ntwo\n".to_string(), &mut partial, "\n", 1)
			.expect_err("second complete event should exceed the remaining capacity");

		assert!(matches!(
			error,
			WebError::StreamLimitExceeded {
				resource: "event count",
				limit: MAX_STREAM_EVENTS
			}
		));
	}

	#[test]
	fn sse_parser_rejects_event_fanout_before_queue_growth() {
		let mut partial = None;

		let error = process_buff_string_sse("one\n\ntwo\n\n".to_string(), &mut partial, 1)
			.expect_err("second SSE event should exceed the remaining capacity");

		assert!(matches!(
			error,
			WebError::StreamLimitExceeded {
				resource: "event count",
				limit: MAX_STREAM_EVENTS
			}
		));
	}
}
