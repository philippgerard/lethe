use crate::Headers;
use crate::webc::{Error, Result};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use reqwest::header::HeaderMap;
use reqwest::{Method, RequestBuilder, Response, StatusCode};
use serde_json::Value;

pub(crate) const MAX_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

pub(crate) async fn collect_limited_text<S>(stream: S, limit: usize) -> Result<String>
where
	S: Stream<Item = core::result::Result<Bytes, reqwest::Error>>,
{
	let mut stream = Box::pin(stream);
	let mut body = Vec::new();
	while let Some(chunk) = stream.as_mut().try_next().await? {
		let next_len = body
			.len()
			.checked_add(chunk.len())
			.ok_or(Error::ResponseBodyTooLarge { limit })?;
		if next_len > limit {
			return Err(Error::ResponseBodyTooLarge { limit });
		}
		body.extend_from_slice(&chunk);
	}
	Ok(String::from_utf8_lossy(&body).into_owned())
}

pub(crate) async fn response_text_limited(response: Response, limit: usize) -> Result<String> {
	if response.content_length().is_some_and(|len| len > limit as u64) {
		return Err(Error::ResponseBodyTooLarge { limit });
	}
	collect_limited_text(response.bytes_stream(), limit).await
}

/// A simple reqwest client wrapper for this library.
#[derive(Debug)]
pub struct WebClient {
	reqwest_client: reqwest::Client,
}

// Implements Default with performance optimizations
impl Default for WebClient {
	fn default() -> Self {
		use std::time::Duration;
		let reqwest_client = reqwest::Client::builder()
			.tcp_nodelay(true)
			.gzip(true)
			.pool_max_idle_per_host(4)
			.http2_keep_alive_interval(Some(Duration::from_secs(20)))
			.http2_keep_alive_timeout(Duration::from_secs(10))
			.http2_keep_alive_while_idle(true)
			.http2_adaptive_window(true)
			.build()
			.expect("Failed to build default reqwest client");
		WebClient { reqwest_client }
	}
}

// region:    --- Constructors

impl WebClient {
	pub fn from_reqwest_client(reqwest_client: reqwest::Client) -> Self {
		WebClient { reqwest_client }
	}
}

// endregion: --- Constructors

// region:    --- Web Method Implementation

impl WebClient {
	pub async fn do_get(&self, url: &str, headers: &Headers) -> Result<WebResponse> {
		let mut reqwest_builder = self.reqwest_client.request(Method::GET, url);

		for (k, v) in headers.iter() {
			reqwest_builder = reqwest_builder.header(k, v);
		}
		let reqwest_res = reqwest_builder.send().await?;

		let response = WebResponse::from_reqwest_response(reqwest_res).await?;

		Ok(response)
	}

	pub async fn do_post(&self, url: &str, headers: &Headers, content: &Value) -> Result<WebResponse> {
		let reqwest_builder = self.new_req_builder(url, headers, content)?;

		let reqwest_res = reqwest_builder.send().await?;

		let response = WebResponse::from_reqwest_response(reqwest_res).await?;

		Ok(response)
	}

	pub fn new_req_builder(&self, url: &str, headers: &Headers, content: &Value) -> Result<RequestBuilder> {
		let method = Method::POST;

		let mut reqwest_builder = self.reqwest_client.request(method, url);
		for (k, v) in headers.iter() {
			reqwest_builder = reqwest_builder.header(k, v);
		}
		reqwest_builder = reqwest_builder.json(content);

		Ok(reqwest_builder)
	}
}
// endregion: --- Web Method Implementation

// region:    --- WebResponse

// NOTE: This is not a non-streaming web response (assumed to be JSON for this library).
//       Streaming is handled with event-source or custom streams (for example, for Cohere).

#[derive(Debug)]
pub struct WebResponse {
	#[allow(unused)]
	pub status: StatusCode,
	pub body: Value,
}

impl WebResponse {
	/// Note 1: For now, assume only a JSON response.
	/// Note 2: Currently, the WebResponse holds a Value (parsed from the entire body), and then the caller
	///         can cherry-pick/deserialize further. In the future, we might consider returning `body: String`
	///         to enable more optimized parsing, allowing for selective parsing constrained by the structure.
	pub(crate) async fn from_reqwest_response(mut res: reqwest::Response) -> Result<WebResponse> {
		let status = res.status();

		if !status.is_success() {
			let headers = res.headers().clone();
			let body = response_text_limited(res, MAX_ERROR_BODY_BYTES).await?;
			tracing::trace!("AI Response failed. Body:\n{body}");
			return Err(Error::ResponseFailedStatus {
				status,
				body,
				headers: Box::new(headers),
			});
		}

		// Move the headers into a new HeaderMap
		let headers = res.headers_mut().drain().filter_map(|(n, v)| n.map(|n| (n, v)));
		let header_map = HeaderMap::from_iter(headers);

		// Capture the body
		let ct = header_map.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or_default();
		let body = response_text_limited(res, MAX_RESPONSE_BODY_BYTES).await?;

		let body = if ct.starts_with("application/json") {
			tracing::trace!("AI Response body:\n{body}");
			let value: Value = serde_json::from_str(&body).map_err(|err| Error::ResponseFailedInvalidJson {
				body,
				cause: err.to_string(),
			})?;
			value
		} else {
			return Err(Error::ResponseFailedNotJson {
				content_type: ct.to_string(),
				body,
			});
		};

		Ok(WebResponse { status, body })
	}
}

// endregion: --- WebResponse

#[cfg(test)]
mod tests {
	use super::*;
	use futures::stream;

	#[tokio::test]
	async fn limited_text_accepts_a_legitimate_chunked_body() {
		let chunks = stream::iter(vec![
			Ok::<_, reqwest::Error>(Bytes::from_static(b"hello ")),
			Ok::<_, reqwest::Error>(Bytes::from_static(b"world")),
		]);

		let body = collect_limited_text(chunks, 11).await.expect("body should fit");

		assert_eq!(body, "hello world");
	}

	#[tokio::test]
	async fn limited_text_rejects_an_oversized_body_before_appending_the_chunk() {
		let chunks = stream::iter(vec![
			Ok::<_, reqwest::Error>(Bytes::from_static(b"hello")),
			Ok::<_, reqwest::Error>(Bytes::from_static(b"!")),
		]);

		let error = collect_limited_text(chunks, 5).await.expect_err("body should exceed the limit");

		assert!(matches!(error, Error::ResponseBodyTooLarge { limit: 5 }));
	}
}
