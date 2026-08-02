//! Shared helpers used by both the SigV4 and API-key Bedrock adapters.

use crate::adapter::{AdapterKind, ServiceType};
use crate::resolver::Endpoint;
use crate::webc::{Error as WebError, MAX_ERROR_BODY_BYTES, response_text_limited};
use crate::{Error, ModelIden, Result};
use reqwest::RequestBuilder;

/// The hostname prefix for the Bedrock runtime endpoint. Regions are interpolated between this
/// prefix and `.amazonaws.com`.
pub(super) const BEDROCK_RUNTIME_HOST_PREFIX: &str = "bedrock-runtime";

/// Curated snapshot of model IDs. Dynamic listing requires the `bedrock` (control plane) API,
/// not `bedrock-runtime`, so we return a hard-coded list here.
pub(super) fn curated_model_names() -> Vec<String> {
	vec![
		"anthropic.claude-sonnet-4-5-20250929-v1:0".to_string(),
		"anthropic.claude-opus-4-1-20250805-v1:0".to_string(),
		"anthropic.claude-haiku-4-5-20251001-v1:0".to_string(),
		"amazon.nova-pro-v1:0".to_string(),
		"amazon.nova-lite-v1:0".to_string(),
		"amazon.nova-micro-v1:0".to_string(),
		"meta.llama3-1-70b-instruct-v1:0".to_string(),
		"mistral.mistral-large-2407-v1:0".to_string(),
	]
}

/// Build the Converse / ConverseStream URL for a given model + service type.
pub(super) fn build_service_url(
	model: &ModelIden,
	service_type: ServiceType,
	endpoint: Endpoint,
	adapter_kind: AdapterKind,
) -> Result<String> {
	let base_url = endpoint.base_url();
	let (_, model_name) = model.model_name.namespace_and_name();
	// Model IDs contain ':' (e.g., anthropic.claude-sonnet-4-5-20250929-v1:0) and must be
	// URL-encoded inside the path segment.
	let encoded = urlencode_path_segment(model_name);

	let url = match service_type {
		ServiceType::Chat => format!("{base_url}model/{encoded}/converse"),
		ServiceType::ChatStream => format!("{base_url}model/{encoded}/converse-stream"),
		ServiceType::Embed => {
			return Err(Error::AdapterNotSupported {
				adapter_kind,
				feature: "embeddings via Converse (use /invoke instead, not yet supported)".to_string(),
			});
		}
	};
	Ok(url)
}

fn urlencode_path_segment(s: &str) -> String {
	let mut out = String::with_capacity(s.len());
	for b in s.bytes() {
		match b {
			b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
			_ => {
				out.push('%');
				out.push_str(&format!("{:02X}", b));
			}
		}
	}
	out
}

/// Turn a `reqwest::RequestBuilder` into a byte stream for the event-stream frame parser.
/// On HTTP error, yields a single error item so the parser surfaces it through the normal
/// error path.
pub(super) fn async_stream_bytes(
	reqwest_builder: RequestBuilder,
) -> impl futures::Stream<Item = std::result::Result<bytes::Bytes, crate::error::BoxError>> + Send {
	use futures::StreamExt;
	async_stream_once(reqwest_builder).flat_map(|result| match result {
		Ok(stream) => stream.boxed(),
		Err(err) => futures::stream::once(async move { Err(err) }).boxed(),
	})
}

fn async_stream_once(
	reqwest_builder: RequestBuilder,
) -> impl futures::Stream<
	Item = std::result::Result<
		futures::stream::BoxStream<'static, std::result::Result<bytes::Bytes, crate::error::BoxError>>,
		crate::error::BoxError,
	>,
> + Send {
	use futures::StreamExt;
	futures::stream::once(async move {
		let resp = reqwest_builder
			.send()
			.await
			.map_err(|e| Box::new(e) as crate::error::BoxError)?;
		let status = resp.status();
		if !status.is_success() {
			let body = match response_text_limited(resp, MAX_ERROR_BODY_BYTES).await {
				Ok(body) => body,
				Err(error @ WebError::ResponseBodyTooLarge { .. }) => {
					return Err(Box::new(error) as crate::error::BoxError);
				}
				Err(error) => format!("Failed to read error body: {error}"),
			};
			let err = crate::Error::HttpError {
				status,
				canonical_reason: status.canonical_reason().unwrap_or("Unknown").to_string(),
				body,
			};
			return Err(Box::new(err) as crate::error::BoxError);
		}
		let bytes: futures::stream::BoxStream<'static, std::result::Result<bytes::Bytes, crate::error::BoxError>> =
			resp.bytes_stream()
				.map(|r| r.map_err(|e| Box::new(e) as crate::error::BoxError))
				.boxed();
		Ok(bytes)
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures::StreamExt;
	use std::io::{Read, Write};
	use std::net::TcpListener;
	use std::thread::JoinHandle;

	fn local_response_request(
		status: &str,
		declared_content_length: usize,
		body: &'static [u8],
	) -> (RequestBuilder, JoinHandle<()>) {
		let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener should bind");
		let address = listener.local_addr().expect("listener should have an address");
		let status = status.to_string();
		let server = std::thread::spawn(move || {
			let (mut socket, _) = listener.accept().expect("test client should connect");
			let mut request = [0_u8; 4096];
			let _ = socket.read(&mut request);
			let headers =
				format!("HTTP/1.1 {status}\r\nContent-Length: {declared_content_length}\r\nConnection: close\r\n\r\n");
			let _ = socket.write_all(headers.as_bytes());
			let _ = socket.write_all(body);
		});
		let client = reqwest::Client::builder().no_proxy().build().expect("test client should build");
		(client.get(format!("http://{address}/converse-stream")), server)
	}

	#[tokio::test]
	async fn ordinary_http_error_preserves_status_and_body() {
		let (request, server) = local_response_request("400 Bad Request", 4, b"nope");
		let mut stream = Box::pin(async_stream_bytes(request));
		let error = stream
			.next()
			.await
			.expect("error response should produce one stream item")
			.expect_err("HTTP error should fail the byte stream");
		server.join().expect("test server should exit");

		let error = error
			.downcast_ref::<crate::Error>()
			.expect("ordinary HTTP errors should retain the genai error type");
		assert!(matches!(
			error,
			crate::Error::HttpError { status, body, .. }
				if *status == reqwest::StatusCode::BAD_REQUEST && body == "nope"
		));
	}

	#[tokio::test]
	async fn oversized_http_error_body_is_rejected_before_collection() {
		let (request, server) = local_response_request("500 Internal Server Error", MAX_ERROR_BODY_BYTES + 1, b"");
		let mut stream = Box::pin(async_stream_bytes(request));
		let error = stream
			.next()
			.await
			.expect("error response should produce one stream item")
			.expect_err("oversized error body should fail the byte stream");
		server.join().expect("test server should exit");

		assert!(matches!(
			error.downcast_ref::<WebError>(),
			Some(WebError::ResponseBodyTooLarge {
				limit: MAX_ERROR_BODY_BYTES
			})
		));
	}
}
