//! This support module is for common constructs and utilities for all the adapter implementations.
//! It should be private to the `crate::adapter::adapters` module.

use crate::ModelIden;
use crate::chat::{ChatOptionsSet, Usage};
use crate::resolver::AuthData;
use crate::{Error, Result};

pub const MAX_CAPTURED_STREAM_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CAPTURED_STREAM_EVENTS: usize = 65_536;
pub const MAX_CAPTURED_TOOL_CALLS: usize = 128;

pub fn get_api_key(auth: AuthData, model: &ModelIden) -> Result<String> {
	auth.single_key_value().map_err(|resolver_error| Error::Resolver {
		model_iden: model.clone(),
		resolver_error,
	})
}

// region:    --- StreamerChatOptions

#[derive(Debug)]
pub struct StreamerOptions {
	pub capture_usage: bool,
	pub capture_reasoning_content: bool,
	pub capture_content: bool,
	pub capture_tool_calls: bool,
	pub model_iden: ModelIden,
}

impl StreamerOptions {
	pub fn new(model_iden: ModelIden, options_set: ChatOptionsSet<'_, '_>) -> Self {
		Self {
			capture_usage: options_set.capture_usage().unwrap_or(false),
			capture_content: options_set.capture_content().unwrap_or(false),
			capture_reasoning_content: options_set.capture_reasoning_content().unwrap_or(false),
			capture_tool_calls: options_set.capture_tool_calls().unwrap_or(false),
			model_iden,
		}
	}
}

// endregion: --- StreamerChatOptions

// region:    --- Streamer Captured Data

#[derive(Debug, Default)]
struct StreamCaptureBudget {
	bytes: usize,
	events: usize,
}

impl StreamCaptureBudget {
	fn record(&mut self, bytes: usize) -> Result<()> {
		self.record_with_limits(bytes, MAX_CAPTURED_STREAM_BYTES, MAX_CAPTURED_STREAM_EVENTS)
	}

	fn record_with_limits(&mut self, bytes: usize, byte_limit: usize, event_limit: usize) -> Result<()> {
		let next_bytes = self.bytes.checked_add(bytes).ok_or(Error::StreamLimitExceeded {
			resource: "capture bytes",
			limit: byte_limit,
		})?;
		if next_bytes > byte_limit {
			return Err(Error::StreamLimitExceeded {
				resource: "capture bytes",
				limit: byte_limit,
			});
		}
		let next_events = self.events.checked_add(1).ok_or(Error::StreamLimitExceeded {
			resource: "capture events",
			limit: event_limit,
		})?;
		if next_events > event_limit {
			return Err(Error::StreamLimitExceeded {
				resource: "capture events",
				limit: event_limit,
			});
		}
		self.bytes = next_bytes;
		self.events = next_events;
		Ok(())
	}
}

#[derive(Debug, Default)]
pub struct StreamerCapturedData {
	pub usage: Option<Usage>,
	pub stop_reason: Option<String>,
	content: Option<String>,
	reasoning_content: Option<String>,
	tool_calls: Option<Vec<crate::chat::ToolCall>>,
	thought_signatures: Option<Vec<String>>,
	budget: StreamCaptureBudget,
}

impl StreamerCapturedData {
	pub fn record_capture(&mut self, bytes: usize) -> Result<()> {
		self.budget.record(bytes)
	}

	pub fn append_content(&mut self, content: &str) -> Result<()> {
		self.record_capture(content.len())?;
		match self.content {
			Some(ref mut captured) => captured.push_str(content),
			None => self.content = Some(content.to_string()),
		}
		Ok(())
	}

	pub fn append_reasoning_content(&mut self, content: &str) -> Result<()> {
		self.record_capture(content.len())?;
		match self.reasoning_content {
			Some(ref mut captured) => captured.push_str(content),
			None => self.reasoning_content = Some(content.to_string()),
		}
		Ok(())
	}

	pub fn push_tool_call(&mut self, tool_call: crate::chat::ToolCall) -> Result<()> {
		self.ensure_tool_call_capacity()?;
		self.record_capture(tool_call.size())?;
		self.push_accounted_tool_call(tool_call)
	}

	/// Store a tool call whose bytes were already charged while its streamed
	/// fragments were accumulated. The count limit is still enforced here.
	pub fn push_accounted_tool_call(&mut self, tool_call: crate::chat::ToolCall) -> Result<()> {
		self.ensure_tool_call_capacity()?;
		match self.tool_calls {
			Some(ref mut captured) => captured.push(tool_call),
			None => self.tool_calls = Some(vec![tool_call]),
		}
		Ok(())
	}

	pub fn merge_tool_call_fragment(
		&mut self,
		index: usize,
		call_id: String,
		fn_name: String,
		arguments: String,
	) -> Result<crate::chat::ToolCall> {
		if index >= MAX_CAPTURED_TOOL_CALLS || index > self.tool_call_count() {
			return Err(Error::InvalidToolCallIndex {
				index,
				limit: MAX_CAPTURED_TOOL_CALLS,
			});
		}
		self.record_capture(call_id.len() + fn_name.len() + arguments.len())?;

		if let Some(existing_call) = self.tool_calls.as_mut().and_then(|calls| calls.get_mut(index)) {
			if let Some(existing_args) = existing_call.fn_arguments.as_str() {
				existing_call.fn_arguments = serde_json::Value::String(format!("{existing_args}{arguments}"));
			}
			if !fn_name.is_empty() {
				existing_call.call_id = call_id;
				existing_call.fn_name = fn_name;
			}
			return Ok(existing_call.clone());
		}

		let tool_call = crate::chat::ToolCall {
			call_id,
			fn_name,
			fn_arguments: serde_json::Value::String(arguments),
			thought_signatures: None,
		};
		self.push_accounted_tool_call(tool_call.clone())?;
		Ok(tool_call)
	}

	pub fn push_thought_signature(&mut self, signature: String) -> Result<()> {
		self.record_capture(signature.len())?;
		match self.thought_signatures {
			Some(ref mut captured) => captured.push(signature),
			None => self.thought_signatures = Some(vec![signature]),
		}
		Ok(())
	}

	pub fn tool_call_count(&self) -> usize {
		self.tool_calls.as_ref().map(Vec::len).unwrap_or_default()
	}

	pub fn has_thought_signatures(&self) -> bool {
		self.thought_signatures
			.as_ref()
			.is_some_and(|signatures| !signatures.is_empty())
	}

	pub fn take_content(&mut self) -> Option<String> {
		self.content.take()
	}

	pub fn take_reasoning_content(&mut self) -> Option<String> {
		self.reasoning_content.take()
	}

	pub fn take_tool_calls(&mut self) -> Option<Vec<crate::chat::ToolCall>> {
		self.tool_calls.take()
	}

	pub fn take_thought_signatures(&mut self) -> Option<Vec<String>> {
		self.thought_signatures.take()
	}

	fn ensure_tool_call_capacity(&self) -> Result<()> {
		if self.tool_call_count() >= MAX_CAPTURED_TOOL_CALLS {
			return Err(Error::StreamLimitExceeded {
				resource: "captured tool calls",
				limit: MAX_CAPTURED_TOOL_CALLS,
			});
		}
		Ok(())
	}
}

// endregion: --- Streamer Captured Data

#[cfg(test)]
mod tests {
	use super::*;
	use crate::chat::ToolCall;
	use serde_json::Value;

	fn tool_call() -> ToolCall {
		ToolCall {
			call_id: "call".to_string(),
			fn_name: "lookup".to_string(),
			fn_arguments: Value::Object(Default::default()),
			thought_signatures: None,
		}
	}

	#[test]
	fn capture_budget_accepts_legitimate_bytes_and_events() {
		let mut budget = StreamCaptureBudget::default();

		budget.record_with_limits(3, 5, 2).expect("first capture should fit");
		budget.record_with_limits(2, 5, 2).expect("boundary capture should fit");

		assert_eq!(budget.bytes, 5);
		assert_eq!(budget.events, 2);
	}

	#[test]
	fn capture_budget_rejects_excess_bytes_without_committing_them() {
		let mut budget = StreamCaptureBudget::default();
		budget.record_with_limits(5, 5, 2).expect("boundary capture should fit");

		let error = budget
			.record_with_limits(1, 5, 2)
			.expect_err("capture should exceed byte limit");

		assert!(matches!(
			error,
			Error::StreamLimitExceeded {
				resource: "capture bytes",
				limit: 5
			}
		));
		assert_eq!(budget.bytes, 5);
		assert_eq!(budget.events, 1);
	}

	#[test]
	fn capture_budget_rejects_excess_events_without_committing_them() {
		let mut budget = StreamCaptureBudget::default();
		budget.record_with_limits(1, 5, 1).expect("first event should fit");

		let error = budget
			.record_with_limits(1, 5, 1)
			.expect_err("capture should exceed event limit");

		assert!(matches!(
			error,
			Error::StreamLimitExceeded {
				resource: "capture events",
				limit: 1
			}
		));
		assert_eq!(budget.bytes, 1);
		assert_eq!(budget.events, 1);
	}

	#[test]
	fn append_content_preserves_legitimate_stream_assembly() {
		let mut captured = StreamerCapturedData::default();

		captured.append_content("hello ").expect("first chunk should fit");
		captured.append_content("world").expect("second chunk should fit");

		assert_eq!(captured.content.as_deref(), Some("hello world"));
	}

	#[test]
	fn append_content_rejects_the_byte_limit_without_partial_append() {
		let mut captured = StreamerCapturedData {
			content: Some("safe".to_string()),
			budget: StreamCaptureBudget {
				bytes: MAX_CAPTURED_STREAM_BYTES,
				events: 1,
			},
			..Default::default()
		};

		let error = captured.append_content("x").expect_err("capture should exceed the byte limit");

		assert!(matches!(
			error,
			Error::StreamLimitExceeded {
				resource: "capture bytes",
				limit: MAX_CAPTURED_STREAM_BYTES
			}
		));
		assert_eq!(captured.content.as_deref(), Some("safe"));
	}

	#[test]
	fn push_tool_call_accepts_a_legitimate_call() {
		let mut captured = StreamerCapturedData::default();

		captured.push_tool_call(tool_call()).expect("first tool call should fit");

		assert_eq!(captured.tool_calls.as_ref().map(Vec::len), Some(1));
	}

	#[test]
	fn push_tool_call_rejects_the_count_limit_without_growth() {
		let mut captured = StreamerCapturedData {
			tool_calls: Some(vec![tool_call(); MAX_CAPTURED_TOOL_CALLS]),
			..Default::default()
		};

		let error = captured
			.push_tool_call(tool_call())
			.expect_err("tool-call count at the limit should fail");

		assert!(matches!(
			error,
			Error::StreamLimitExceeded {
				resource: "captured tool calls",
				limit: MAX_CAPTURED_TOOL_CALLS
			}
		));
		assert_eq!(
			captured.tool_calls.as_ref().map(Vec::len),
			Some(MAX_CAPTURED_TOOL_CALLS)
		);
	}
}
