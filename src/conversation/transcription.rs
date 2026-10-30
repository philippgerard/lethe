use std::borrow::Cow;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::blocking::multipart;
use serde_json::json;
use thiserror::Error;

use crate::config::Settings;

pub const OPENAI_TRANSCRIPTIONS_URL: &str = "https://api.openai.com/v1/audio/transcriptions";
pub const OPENROUTER_TRANSCRIPTIONS_URL: &str = "https://openrouter.ai/api/v1/audio/transcriptions";
// Mistral's transcription endpoint is OpenAI-compatible: multipart/form-data
// (model + file), bearer auth, OpenAI-shaped JSON with a top-level `text` field.
pub const MISTRAL_TRANSCRIPTIONS_URL: &str = "https://api.mistral.ai/v1/audio/transcriptions";

pub const DEFAULT_OPENAI_MODEL: &str = "whisper-1";
pub const DEFAULT_OPENROUTER_MODEL: &str = "openai/whisper-large-v3";
pub const DEFAULT_LOCAL_MODEL: &str = "base";
// Voxtral Mini Transcribe — strong multilingual (DE/FR/IT) speech-to-text.
pub const DEFAULT_MISTRAL_MODEL: &str = "voxtral-mini-latest";

const CONVERTED_AUDIO_LIMIT: u64 = 25 * 1024 * 1024;
const AUDIO_CONVERSION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum TranscriptionError {
    #[error("unsupported TRANSCRIPTION_PROVIDER '{0}'. Use local, openai, openrouter, or mistral")]
    UnsupportedProvider(String),
    #[error(
        "speech-to-text is not configured. Set OPENROUTER_API_KEY or OPENAI_API_KEY, or set TRANSCRIPTION_PROVIDER=local with a local Whisper CLI installed"
    )]
    NotConfigured,
    #[error("{0} is required for {1} transcription")]
    MissingApiKey(&'static str, String),
    #[error("cannot transcribe an empty audio file")]
    EmptyAudio,
    #[error("TRANSCRIPTION_LOCAL_COMMAND cannot be empty")]
    EmptyLocalCommand,
    #[error(
        "local Whisper command '{0}' was not found. Install openai-whisper or set TRANSCRIPTION_LOCAL_COMMAND"
    )]
    LocalCommandNotFound(String),
    #[error("local Whisper failed with exit code {code}: {stderr}")]
    LocalWhisperFailed { code: i32, stderr: String },
    #[error("local Whisper returned an empty transcription")]
    EmptyLocalTranscript,
    #[error(
        "ffmpeg was not found. Install ffmpeg to transcribe CAF, AIFF, or AMR with a remote provider"
    )]
    FfmpegNotFound,
    #[error(
        "ffmpeg could not convert this audio to WAV. Check that the CAF, AIFF, or AMR recording is valid and ffmpeg supports its codec"
    )]
    AudioConversionFailed,
    #[error("audio conversion exceeded {seconds} seconds. Send a shorter recording")]
    AudioConversionTimedOut { seconds: u64 },
    #[error("converted audio exceeds the {max_bytes}-byte limit. Send a shorter recording")]
    ConvertedAudioTooLarge { max_bytes: u64 },
    #[error("{provider} transcription failed: {message}")]
    Provider { provider: String, message: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

pub type TranscriptionResult<T> = Result<T, TranscriptionError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TranscriptionProvider {
    OpenRouter,
    OpenAi,
    Mistral,
    Local,
}

impl TranscriptionProvider {
    pub fn parse(value: &str) -> TranscriptionResult<Option<Self>> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(None),
            "openrouter" => Ok(Some(Self::OpenRouter)),
            "openai" => Ok(Some(Self::OpenAi)),
            "mistral" => Ok(Some(Self::Mistral)),
            "local" => Ok(Some(Self::Local)),
            other => Err(TranscriptionError::UnsupportedProvider(other.to_string())),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenRouter => "openrouter",
            Self::OpenAi => "openai",
            Self::Mistral => "mistral",
            Self::Local => "local",
        }
    }
}

pub fn choose_transcription_provider(
    settings: &Settings,
) -> TranscriptionResult<TranscriptionProvider> {
    if let Some(provider) = TranscriptionProvider::parse(&settings.transcription.provider)? {
        return Ok(provider);
    }
    // A configured LLM_API_BASE means we're talking to an OpenAI-compatible proxy
    // (the hosted metering proxy) that fronts OpenRouter. Its JSON
    // /audio/transcriptions route is the only egress, and the credential is the
    // per-user proxy token (carried in OPENAI_API_KEY), so use the OpenRouter path
    // there rather than picking OpenAI and calling api.openai.com directly.
    if !settings.llm.llm_api_base.trim().is_empty()
        && (configured_api_key(&settings.llm.openrouter_api_key)
            || configured_api_key(&settings.llm.openai_api_key))
    {
        return Ok(TranscriptionProvider::OpenRouter);
    }
    if configured_api_key(&settings.llm.openrouter_api_key) {
        return Ok(TranscriptionProvider::OpenRouter);
    }
    if configured_api_key(&settings.llm.openai_api_key) {
        return Ok(TranscriptionProvider::OpenAi);
    }
    if configured_api_key(&settings.llm.mistral_api_key) {
        return Ok(TranscriptionProvider::Mistral);
    }
    if local_whisper_available(&settings.transcription.local_command) {
        return Ok(TranscriptionProvider::Local);
    }
    Err(TranscriptionError::NotConfigured)
}

pub fn default_model_for_provider(provider: TranscriptionProvider) -> &'static str {
    match provider {
        TranscriptionProvider::OpenRouter => DEFAULT_OPENROUTER_MODEL,
        TranscriptionProvider::OpenAi => DEFAULT_OPENAI_MODEL,
        TranscriptionProvider::Mistral => DEFAULT_MISTRAL_MODEL,
        TranscriptionProvider::Local => DEFAULT_LOCAL_MODEL,
    }
}

pub fn infer_audio_format(filename: &str, mime_type: Option<&str>) -> String {
    if let Some(mime_type) = mime_type
        && let Some(format) = mime_to_format(mime_type.split(';').next().unwrap_or("").trim())
    {
        return format.to_string();
    }
    let suffix = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if suffix.is_empty() {
        return "ogg".to_string();
    }
    extension_alias(&suffix).unwrap_or(&suffix).to_string()
}

pub fn filename_for_upload(filename: &str, audio_format: &str) -> String {
    // Telegram controls this display name. Treat both Unix and Windows path
    // separators as boundaries even when Lethe is running on the other OS.
    let basename = filename.rsplit(['/', '\\']).next().unwrap_or_default();
    let base = Path::new(basename)
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty() && *value != "." && *value != "..")
        .unwrap_or("telegram_audio");
    format!("{base}.{}", safe_audio_format(audio_format))
}

fn safe_audio_format(audio_format: &str) -> &str {
    let audio_format = audio_format.trim().trim_start_matches('.');
    if !audio_format.is_empty()
        && audio_format
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
    {
        audio_format
    } else {
        "ogg"
    }
}

pub fn mime_type_for_format(audio_format: &str) -> &'static str {
    match audio_format {
        "aac" => "audio/aac",
        "aiff" => "audio/aiff",
        "amr" => "audio/amr",
        "caf" => "audio/x-caf",
        "flac" => "audio/flac",
        "m4a" => "audio/mp4",
        "mp3" => "audio/mpeg",
        "mp4" => "audio/mp4",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",
        "webm" => "audio/webm",
        _ => "application/octet-stream",
    }
}

pub fn transcribe_audio(
    audio_bytes: &[u8],
    filename: &str,
    mime_type: Option<&str>,
    settings: &Settings,
) -> TranscriptionResult<String> {
    if audio_bytes.is_empty() {
        return Err(TranscriptionError::EmptyAudio);
    }
    let provider = choose_transcription_provider(settings)?;
    let model = if settings.transcription.model.trim().is_empty() {
        default_model_for_provider(provider).to_string()
    } else {
        settings.transcription.model.trim().to_string()
    };
    let language = settings.transcription.language.trim();
    let language = if language.is_empty() {
        None
    } else {
        Some(language)
    };
    let audio_format = infer_audio_format(filename, mime_type);
    let prepared = prepare_audio(
        provider,
        audio_bytes,
        &audio_format,
        mime_type,
        Path::new("ffmpeg"),
    )?;
    let audio_format = prepared.format;
    let mime_type = prepared.mime_type;

    match provider {
        TranscriptionProvider::Local => transcribe_local_whisper(
            prepared.bytes.as_ref(),
            filename,
            audio_format,
            &model,
            language,
            &settings.transcription.local_command,
        ),
        TranscriptionProvider::OpenRouter => transcribe_openrouter(
            prepared.bytes.as_ref(),
            audio_format,
            &model,
            language,
            &openrouter_transcriptions_url(settings),
            &openrouter_transcription_key(settings)?,
        ),
        TranscriptionProvider::OpenAi => transcribe_multipart(
            prepared.bytes,
            filename,
            audio_format,
            mime_type,
            &model,
            language,
            OPENAI_TRANSCRIPTIONS_URL,
            &api_key_for_provider(provider, settings)?,
            "OpenAI",
        ),
        TranscriptionProvider::Mistral => transcribe_multipart(
            prepared.bytes,
            filename,
            audio_format,
            mime_type,
            &model,
            language,
            MISTRAL_TRANSCRIPTIONS_URL,
            &api_key_for_provider(provider, settings)?,
            "Mistral",
        ),
    }
}

struct PreparedAudio<'a> {
    bytes: Cow<'a, [u8]>,
    format: &'a str,
    mime_type: Option<&'a str>,
}

fn prepare_audio<'a>(
    provider: TranscriptionProvider,
    audio_bytes: &'a [u8],
    audio_format: &'a str,
    mime_type: Option<&'a str>,
    ffmpeg: &Path,
) -> TranscriptionResult<PreparedAudio<'a>> {
    if provider != TranscriptionProvider::Local && matches!(audio_format, "caf" | "aiff" | "amr") {
        return Ok(PreparedAudio {
            bytes: Cow::Owned(convert_native_audio(
                audio_bytes,
                audio_format,
                ffmpeg,
                CONVERTED_AUDIO_LIMIT,
                AUDIO_CONVERSION_TIMEOUT,
            )?),
            format: "wav",
            mime_type: Some("audio/wav"),
        });
    }
    let canonical_mime = mime_type_for_format(audio_format);
    Ok(PreparedAudio {
        bytes: Cow::Borrowed(audio_bytes),
        format: audio_format,
        mime_type: if canonical_mime == "application/octet-stream" {
            mime_type
        } else {
            Some(canonical_mime)
        },
    })
}

// Kill and reap the converter before its private scratch directory is removed,
// including on I/O errors and unwinding.
struct AudioConverter(Child);

impl Drop for AudioConverter {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn convert_native_audio(
    audio_bytes: &[u8],
    audio_format: &str,
    ffmpeg: &Path,
    max_bytes: u64,
    timeout: Duration,
) -> TranscriptionResult<Vec<u8>> {
    let scratch = tempfile::Builder::new().prefix("lethe-audio-").tempdir()?;
    let input = scratch.path().join("input");
    let output = scratch.path().join("output.wav");
    fs::write(&input, audio_bytes)?;
    let child = Command::new(ffmpeg)
        .args(["-nostdin", "-hide_banner", "-loglevel", "error"])
        // Fix the demuxer rather than letting untrusted input select a playlist
        // or network protocol. All paths are server-owned, never display names.
        .args([
            "-protocol_whitelist",
            "file",
            "-f",
            audio_format,
            "-threads",
            "1",
            "-i",
        ])
        .arg(&input)
        .args(["-map", "0:a:0", "-vn", "-ac", "1", "-ar", "16000"])
        .args(["-c:a", "pcm_s16le", "-threads", "1", "-f", "wav", "-fs"])
        .arg(max_bytes.to_string())
        .arg(&output)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        // Do not expose untrusted decoder output or accumulate unbounded logs.
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                TranscriptionError::FfmpegNotFound
            } else {
                TranscriptionError::Io(error)
            }
        })?;
    let mut converter = AudioConverter(child);
    let started = Instant::now();
    loop {
        // ffmpeg's -fs stops after a packet crosses the boundary. Reject the
        // boundary too: that output may be truncated even on a successful exit.
        if output.exists() && fs::metadata(&output)?.len() >= max_bytes {
            return Err(TranscriptionError::ConvertedAudioTooLarge { max_bytes });
        }
        if let Some(status) = converter.0.try_wait()? {
            if !status.success() {
                return Err(TranscriptionError::AudioConversionFailed);
            }
            break;
        }
        if started.elapsed() >= timeout {
            return Err(TranscriptionError::AudioConversionTimedOut {
                seconds: timeout.as_secs(),
            });
        }
        std::thread::sleep(Duration::from_millis(10).min(timeout));
    }
    let mut bytes = Vec::new();
    let output = fs::File::open(output).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            TranscriptionError::AudioConversionFailed
        } else {
            TranscriptionError::Io(error)
        }
    })?;
    output
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 >= max_bytes {
        return Err(TranscriptionError::ConvertedAudioTooLarge { max_bytes });
    }
    if !wav_contains_audio(&bytes) {
        return Err(TranscriptionError::AudioConversionFailed);
    }
    Ok(bytes)
}

fn wav_contains_audio(bytes: &[u8]) -> bool {
    if bytes.len() < 12 || &bytes[..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return false;
    }
    let mut offset = 12;
    while let Some(header) = bytes.get(offset..offset + 8) {
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let end = offset + 8 + length;
        if end > bytes.len() {
            return false;
        }
        if &header[..4] == b"data" {
            return length > 0;
        }
        offset = end + length % 2;
    }
    false
}

fn transcribe_openrouter(
    audio_bytes: &[u8],
    audio_format: &str,
    model: &str,
    language: Option<&str>,
    url: &str,
    api_key: &str,
) -> TranscriptionResult<String> {
    let mut payload = json!({
        "model": model,
        "input_audio": {
            "data": BASE64.encode(audio_bytes),
            "format": audio_format,
        },
    });
    if let Some(language) = language {
        payload["language"] = json!(language);
    }
    let response = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60))
        .build()?
        .post(url)
        .bearer_auth(api_key)
        .json(&payload)
        .send()?;
    if !response.status().is_success() {
        return Err(provider_error("OpenRouter", response));
    }
    let text = response
        .json::<serde_json::Value>()?
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if text.is_empty() {
        Err(TranscriptionError::Provider {
            provider: "OpenRouter".to_string(),
            message: "returned an empty transcription".to_string(),
        })
    } else {
        Ok(text)
    }
}

/// Shared multipart `/v1/audio/transcriptions` path for OpenAI-compatible
/// providers (OpenAI Whisper and Mistral Voxtral). Both accept the same
/// multipart form (model + file + optional language) and return an
/// OpenAI-shaped JSON body with a top-level `text` field; only the endpoint
/// URL, bearer key, and provider label differ.
fn transcribe_multipart(
    audio_bytes: Cow<'_, [u8]>,
    filename: &str,
    audio_format: &str,
    mime_type: Option<&str>,
    model: &str,
    language: Option<&str>,
    url: &str,
    api_key: &str,
    provider_label: &str,
) -> TranscriptionResult<String> {
    let upload_name = filename_for_upload(filename, audio_format);
    let part = multipart::Part::bytes(audio_bytes.into_owned())
        .file_name(upload_name)
        .mime_str(mime_type.unwrap_or_else(|| mime_type_for_format(audio_format)))?;
    let mut form = multipart::Form::new()
        .text("model", model.to_string())
        .text("response_format", "json".to_string())
        .part("file", part);
    if let Some(language) = language {
        form = form.text("language", language.to_string());
    }
    let response = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60))
        .build()?
        .post(url)
        .bearer_auth(api_key)
        .multipart(form)
        .send()?;
    if !response.status().is_success() {
        return Err(provider_error(provider_label, response));
    }
    let text = response
        .json::<serde_json::Value>()?
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if text.is_empty() {
        Err(TranscriptionError::Provider {
            provider: provider_label.to_string(),
            message: "returned an empty transcription".to_string(),
        })
    } else {
        Ok(text)
    }
}

fn transcribe_local_whisper(
    audio_bytes: &[u8],
    _untrusted_filename: &str,
    audio_format: &str,
    model: &str,
    language: Option<&str>,
    command: &str,
) -> TranscriptionResult<String> {
    let command_parts = split_command(command);
    if command_parts.is_empty() {
        return Err(TranscriptionError::EmptyLocalCommand);
    }

    let tmpdir = tempfile::Builder::new().prefix("lethe-stt-").tempdir()?;
    // Local filesystem paths are server-owned. The remote display name is
    // intentionally ignored rather than reused as a path component.
    let audio_path = tmpdir.path().join(format!(
        "telegram_audio.{}",
        safe_audio_format(audio_format)
    ));
    fs::write(&audio_path, audio_bytes)?;

    run_local_whisper_command(&command_parts, &audio_path, tmpdir.path(), model, language)
}

fn run_local_whisper_command(
    command_parts: &[String],
    audio_path: &Path,
    output_dir: &Path,
    model: &str,
    language: Option<&str>,
) -> TranscriptionResult<String> {
    let mut command = Command::new(&command_parts[0]);
    for arg in &command_parts[1..] {
        command.arg(arg);
    }
    command
        .arg(audio_path)
        .arg("--model")
        .arg(model)
        .arg("--output_format")
        .arg("txt")
        .arg("--output_dir")
        .arg(output_dir);
    if let Some(language) = language {
        command.arg("--language").arg(language);
    }

    let output = command.output().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            TranscriptionError::LocalCommandNotFound(command_parts[0].clone())
        } else {
            TranscriptionError::Io(error)
        }
    })?;
    if !output.status.success() {
        let mut stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if stderr.chars().count() > 500 {
            stderr = stderr.chars().take(500).collect::<String>() + "...";
        }
        return Err(TranscriptionError::LocalWhisperFailed {
            code: output.status.code().unwrap_or(-1),
            stderr,
        });
    }

    let transcript_path = audio_path.with_extension("txt");
    let text = if transcript_path.exists() {
        fs::read_to_string(transcript_path)?.trim().to_string()
    } else {
        extract_local_whisper_stdout(&String::from_utf8_lossy(&output.stdout))
    };
    if text.is_empty() {
        Err(TranscriptionError::EmptyLocalTranscript)
    } else {
        Ok(text)
    }
}

pub fn extract_local_whisper_stdout(stdout: &str) -> String {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line.starts_with('[') && line.contains(']') {
                line.split_once(']')
                    .map(|(_, rest)| rest.trim())
                    .unwrap_or(line)
                    .to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Endpoint for the OpenRouter-format JSON transcription request. Behind the
/// hosted metering proxy, `LLM_API_BASE` fronts OpenRouter — the proxy forwards
/// `/audio/transcriptions` upstream with the real key — so prefer it; a direct
/// setup falls back to OpenRouter's public endpoint.
fn openrouter_transcriptions_url(settings: &Settings) -> String {
    let base = settings.llm.llm_api_base.trim().trim_end_matches('/');
    if base.is_empty() {
        OPENROUTER_TRANSCRIPTIONS_URL.to_string()
    } else {
        format!("{base}/audio/transcriptions")
    }
}

/// Credential for the OpenRouter transcription call. A direct setup uses
/// `OPENROUTER_API_KEY`; behind the proxy the per-user proxy token arrives in
/// `OPENAI_API_KEY` (the proxy swaps in the real upstream key), so fall back to
/// it when no dedicated OpenRouter key is set.
fn openrouter_transcription_key(settings: &Settings) -> TranscriptionResult<String> {
    if configured_api_key(&settings.llm.openrouter_api_key) {
        Ok(settings.llm.openrouter_api_key.clone())
    } else if configured_api_key(&settings.llm.openai_api_key) {
        Ok(settings.llm.openai_api_key.clone())
    } else {
        Err(TranscriptionError::MissingApiKey(
            "OPENROUTER_API_KEY",
            "openrouter".to_string(),
        ))
    }
}

fn api_key_for_provider(
    provider: TranscriptionProvider,
    settings: &Settings,
) -> TranscriptionResult<String> {
    match provider {
        TranscriptionProvider::OpenRouter => {
            if configured_api_key(&settings.llm.openrouter_api_key) {
                Ok(settings.llm.openrouter_api_key.clone())
            } else {
                Err(TranscriptionError::MissingApiKey(
                    "OPENROUTER_API_KEY",
                    provider.as_str().to_string(),
                ))
            }
        }
        TranscriptionProvider::OpenAi => {
            if configured_api_key(&settings.llm.openai_api_key) {
                Ok(settings.llm.openai_api_key.clone())
            } else {
                Err(TranscriptionError::MissingApiKey(
                    "OPENAI_API_KEY",
                    provider.as_str().to_string(),
                ))
            }
        }
        TranscriptionProvider::Mistral => {
            if configured_api_key(&settings.llm.mistral_api_key) {
                Ok(settings.llm.mistral_api_key.clone())
            } else {
                Err(TranscriptionError::MissingApiKey(
                    "MISTRAL_API_KEY",
                    provider.as_str().to_string(),
                ))
            }
        }
        TranscriptionProvider::Local => Ok(String::new()),
    }
}

fn configured_api_key(api_key: &str) -> bool {
    !matches!(api_key.trim().to_ascii_lowercase().as_str(), "" | "local")
}

fn local_whisper_available(command: &str) -> bool {
    split_command(command)
        .first()
        .is_some_and(|name| command_exists(name))
}

fn command_exists(name: &str) -> bool {
    if name.contains('/') {
        return Path::new(name).exists();
    }
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|path| path.join(name).exists()))
}

fn provider_error(provider: &str, response: reqwest::blocking::Response) -> TranscriptionError {
    let status = response.status();
    let mut message = response.text().unwrap_or_default();
    if message.chars().count() > 500 {
        message = message.chars().take(500).collect::<String>() + "...";
    }
    TranscriptionError::Provider {
        provider: provider.to_string(),
        message: format!("HTTP {status}: {}", message.trim()),
    }
}

fn mime_to_format(mime: &str) -> Option<&'static str> {
    match mime.to_ascii_lowercase().as_str() {
        "audio/aac" => Some("aac"),
        "audio/aiff" | "audio/x-aiff" | "audio/aif" => Some("aiff"),
        "audio/amr" | "audio/amr-wb" => Some("amr"),
        "audio/caf" | "audio/x-caf" => Some("caf"),
        "audio/flac" => Some("flac"),
        "audio/m4a" | "audio/x-m4a" | "audio/mp4" => Some("m4a"),
        "audio/mpeg" | "audio/mp3" => Some("mp3"),
        "audio/ogg" | "audio/opus" => Some("ogg"),
        "audio/wav" | "audio/wave" | "audio/x-wav" | "audio/x-pn-wav" | "audio/vnd.wave" => {
            Some("wav")
        }
        "audio/webm" | "video/webm" => Some("webm"),
        "video/mp4" => Some("mp4"),
        _ => None,
    }
}

fn extension_alias(extension: &str) -> Option<&'static str> {
    match extension {
        "aif" | "aifc" => Some("aiff"),
        "wave" => Some("wav"),
        "oga" | "opus" => Some("ogg"),
        "mpeg" | "mpga" => Some("mp3"),
        _ => None,
    }
}

fn split_command(command: &str) -> Vec<String> {
    command
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn settings() -> Settings {
        let mut settings = crate::config::test_settings(&PathBuf::from("/tmp/lethe"));
        settings.llm.llm_model.clear();
        settings
    }

    #[test]
    fn choose_provider_prefers_configured_keys_and_explicit_provider() {
        let mut settings = settings();
        settings.llm.openrouter_api_key = "or-key".to_string();
        settings.llm.openai_api_key = "oa-key".to_string();
        assert_eq!(
            choose_transcription_provider(&settings).unwrap(),
            TranscriptionProvider::OpenRouter
        );

        settings.transcription.provider = "openai".to_string();
        assert_eq!(
            choose_transcription_provider(&settings).unwrap(),
            TranscriptionProvider::OpenAi
        );

        settings.transcription.provider = "local".to_string();
        assert_eq!(
            choose_transcription_provider(&settings).unwrap(),
            TranscriptionProvider::Local
        );

        settings.transcription.provider = "mistral".to_string();
        assert_eq!(
            choose_transcription_provider(&settings).unwrap(),
            TranscriptionProvider::Mistral
        );
    }

    #[test]
    fn mistral_provider_parses_round_trips_and_auto_selects() {
        assert_eq!(
            TranscriptionProvider::parse("mistral").unwrap(),
            Some(TranscriptionProvider::Mistral)
        );
        assert_eq!(
            TranscriptionProvider::parse("MISTRAL").unwrap(),
            Some(TranscriptionProvider::Mistral)
        );
        assert_eq!(TranscriptionProvider::Mistral.as_str(), "mistral");

        // With only a Mistral key configured (no openrouter/openai, no local
        // whisper, empty provider), auto-detection selects Mistral.
        let mut settings = settings();
        settings.transcription.provider.clear();
        settings.llm.openrouter_api_key = String::new();
        settings.llm.openai_api_key = String::new();
        settings.llm.mistral_api_key = "mi-key".to_string();
        settings.transcription.local_command = "/definitely/not/a/whisper".to_string();
        assert_eq!(
            choose_transcription_provider(&settings).unwrap(),
            TranscriptionProvider::Mistral
        );
        assert_eq!(
            api_key_for_provider(TranscriptionProvider::Mistral, &settings).unwrap(),
            "mi-key"
        );

        // An unset key surfaces a MISSING MISTRAL_API_KEY error.
        settings.llm.mistral_api_key = String::new();
        assert!(matches!(
            api_key_for_provider(TranscriptionProvider::Mistral, &settings),
            Err(TranscriptionError::MissingApiKey("MISTRAL_API_KEY", _))
        ));
    }

    #[test]
    fn choose_provider_rejects_unknown_provider_and_placeholder_keys() {
        let mut settings = settings();
        settings.transcription.provider = "anthropic".to_string();
        assert!(matches!(
            choose_transcription_provider(&settings),
            Err(TranscriptionError::UnsupportedProvider(_))
        ));

        settings.transcription.provider.clear();
        settings.llm.openai_api_key = "local".to_string();
        settings.llm.openrouter_api_key = String::new();
        settings.transcription.local_command = "/definitely/not/a/whisper".to_string();
        assert!(matches!(
            choose_transcription_provider(&settings),
            Err(TranscriptionError::NotConfigured)
        ));
    }

    #[test]
    fn proxy_base_routes_transcription_through_openrouter() {
        // Hosted container: only the proxy token (in OPENAI_API_KEY) + LLM_API_BASE
        // are set. Transcription must select OpenRouter and POST through the proxy,
        // not pick OpenAI and call api.openai.com with an invalid key.
        let mut settings = settings();
        settings.llm.openrouter_api_key = String::new();
        settings.llm.openai_api_key = "proxy-token".to_string();
        settings.llm.llm_api_base = "http://172.17.0.1:9787/llm/v1".to_string();
        assert_eq!(
            choose_transcription_provider(&settings).unwrap(),
            TranscriptionProvider::OpenRouter
        );
        assert_eq!(
            openrouter_transcriptions_url(&settings),
            "http://172.17.0.1:9787/llm/v1/audio/transcriptions"
        );
        assert_eq!(
            openrouter_transcription_key(&settings).unwrap(),
            "proxy-token"
        );
    }

    #[test]
    fn infer_audio_format_from_mime_type_and_extension() {
        assert_eq!(infer_audio_format("voice.oga", Some("audio/ogg")), "ogg");
        assert_eq!(infer_audio_format("song.mpga", None), "mp3");
        assert_eq!(infer_audio_format("clip.webm", None), "webm");
        assert_eq!(infer_audio_format("", None), "ogg");
        assert_eq!(mime_type_for_format("m4a"), "audio/mp4");
    }

    #[test]
    fn upload_filename_matches_normalized_format() {
        assert_eq!(filename_for_upload("voice.oga", "ogg"), "voice.ogg");
        assert_eq!(filename_for_upload("voice.ogg", "ogg"), "voice.ogg");
        assert_eq!(filename_for_upload("", "ogg"), "telegram_audio.ogg");
    }

    #[test]
    fn upload_filename_strips_path_components_and_unsafe_formats() {
        assert_eq!(
            filename_for_upload("../../outside.ogg", "ogg"),
            "outside.ogg"
        );
        assert_eq!(
            filename_for_upload("/tmp/outside.ogg", "ogg"),
            "outside.ogg"
        );
        assert_eq!(
            filename_for_upload(r"..\..\outside.ogg", "ogg"),
            "outside.ogg"
        );
        assert_eq!(filename_for_upload("voice.ogg", "../../wav"), "voice.ogg");
    }

    #[test]
    fn local_transcription_never_writes_to_the_remote_filename() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside.ogg");

        let result = transcribe_local_whisper(
            b"audio",
            outside.to_str().unwrap(),
            "ogg",
            "base",
            None,
            "true",
        );

        assert!(matches!(
            result,
            Err(TranscriptionError::EmptyLocalTranscript)
        ));
        assert!(!outside.exists());
    }

    #[test]
    fn local_whisper_stdout_strips_timestamps() {
        let stdout = "[00:00.000 --> 00:01.000] hello\nplain line";
        assert_eq!(extract_local_whisper_stdout(stdout), "hello\nplain line");
    }

    #[test]
    fn empty_or_truncated_wav_is_not_forwarded() {
        let mut wav = b"RIFF\x24\0\0\0WAVEfmt \x10\0\0\0\x01\0\x01\0\x80\x3e\0\0\0\x7d\0\0\x02\0\x10\0data\0\0\0\0".to_vec();
        assert!(!wav_contains_audio(&wav));
        wav[40..44].copy_from_slice(&2_u32.to_le_bytes());
        assert!(!wav_contains_audio(&wav));
        wav.extend_from_slice(&[0, 0]);
        assert!(wav_contains_audio(&wav));
    }

    #[test]
    fn native_mime_and_extension_aliases_are_canonicalized() {
        for (mime, expected) in [
            ("audio/x-m4a", "m4a"),
            ("audio/x-caf", "caf"),
            ("audio/aiff", "aiff"),
            ("audio/x-aiff", "aiff"),
            ("audio/amr", "amr"),
            ("audio/x-wav", "wav"),
            ("audio/x-pn-wav", "wav"),
            ("audio/vnd.wave", "wav"),
            ("AUDIO/WAVE; codecs=pcm", "wav"),
        ] {
            assert_eq!(infer_audio_format("unhelpful.bin", Some(mime)), expected);
        }
        for (name, expected) in [
            ("voice.CAF", "caf"),
            ("voice.AIF", "aiff"),
            ("voice.aifc", "aiff"),
            ("voice.AMR", "amr"),
            ("voice.wave", "wav"),
        ] {
            assert_eq!(infer_audio_format(name, None), expected);
        }
    }

    #[test]
    fn missing_ffmpeg_is_an_actionable_error_for_each_native_format() {
        let tmp = tempfile::tempdir().unwrap();
        for format in ["caf", "aiff", "amr"] {
            assert!(matches!(
                prepare_audio(
                    TranscriptionProvider::OpenAi,
                    b"native",
                    format,
                    None,
                    &tmp.path().join("no-ffmpeg"),
                ),
                Err(TranscriptionError::FfmpegNotFound)
            ));
        }
    }

    #[cfg(unix)]
    fn converter_script(directory: &Path, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = directory.join("converter");
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn conversion_failures_are_bounded_and_clean_up_scratch() {
        let tmp = tempfile::tempdir().unwrap();
        // Record the server-owned input path so cleanup is checked after an
        // actual failed child process, not just after successful conversion.
        let marker = tmp.path().join("input-path");
        let script = converter_script(
            tmp.path(),
            &format!(
                "while [ \"$1\" != '-i' ]; do shift; done\nshift\nprintf '%s' \"$1\" > '{}'\nexit 1",
                marker.display()
            ),
        );
        let result = convert_native_audio(
            b"invalid",
            "caf",
            &script,
            CONVERTED_AUDIO_LIMIT,
            AUDIO_CONVERSION_TIMEOUT,
        );
        assert!(matches!(
            result,
            Err(TranscriptionError::AudioConversionFailed)
        ));
        let input = fs::read_to_string(marker).unwrap();
        assert!(!Path::new(&input).parent().unwrap().exists());

        let script = converter_script(tmp.path(), "exec sleep 5");
        assert!(matches!(
            convert_native_audio(b"invalid", "caf", &script, 1024, Duration::from_millis(20)),
            Err(TranscriptionError::AudioConversionTimedOut { .. })
        ));

        let script = converter_script(
            tmp.path(),
            "for output do :; done\nprintf '1234567890123456' > \"$output\"",
        );
        assert!(matches!(
            convert_native_audio(b"invalid", "caf", &script, 16, AUDIO_CONVERSION_TIMEOUT),
            Err(TranscriptionError::ConvertedAudioTooLarge { max_bytes: 16 })
        ));
        // Successful exit alone is insufficient: absent/invalid output is not
        // forwarded to a provider, and cannot turn into an empty fallback.
        for body in [
            "exit 0",
            "for output do :; done\nprintf 'bad' > \"$output\"",
        ] {
            let script = converter_script(tmp.path(), body);
            assert!(matches!(
                convert_native_audio(b"invalid", "caf", &script, 1024, AUDIO_CONVERSION_TIMEOUT),
                Err(TranscriptionError::AudioConversionFailed)
            ));
        }
    }

    #[test]
    fn real_native_audio_normalizes_to_pcm_wav_when_ffmpeg_is_installed() {
        let available = Command::new("ffmpeg")
            .arg("-version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        match available {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            result => assert!(result.unwrap().success()),
        }
        let tmp = tempfile::tempdir().unwrap();
        for (format, codec) in [("caf", "pcm_s16le"), ("aiff", "pcm_s16be"), ("amr", "")] {
            let input = tmp.path().join(format!("fixture.{format}"));
            if format == "amr" {
                // Valid AMR-NB 4.75-kbit speech frames (95 payload bits each).
                // Decode-only ffmpeg installations need no optional AMR encoder.
                let mut amr = b"#!AMR\n".to_vec();
                for _ in 0..10 {
                    amr.push(0x04);
                    amr.extend_from_slice(&[0; 12]);
                }
                fs::write(&input, amr).unwrap();
            } else {
                let status = Command::new("ffmpeg")
                    .args(["-nostdin", "-hide_banner", "-loglevel", "error"])
                    .args(["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=8000"])
                    .args(["-t", "0.1", "-c:a", codec, "-f", format])
                    .arg(&input)
                    .status()
                    .unwrap();
                assert!(status.success());
            }
            let original = fs::read(input).unwrap();
            for provider in [
                TranscriptionProvider::OpenAi,
                TranscriptionProvider::OpenRouter,
                TranscriptionProvider::Mistral,
            ] {
                let prepared = prepare_audio(
                    provider,
                    &original,
                    format,
                    Some(mime_type_for_format(format)),
                    Path::new("ffmpeg"),
                )
                .unwrap();
                assert_eq!(prepared.format, "wav");
                assert_eq!(prepared.mime_type, Some("audio/wav"));
                assert_eq!(
                    filename_for_upload("voice.caf", prepared.format),
                    "voice.wav"
                );
                let wav = prepared.bytes.as_ref();
                assert_eq!(&wav[..4], b"RIFF");
                assert_eq!(&wav[8..12], b"WAVE");
                assert_eq!(
                    u32::from_le_bytes(wav[4..8].try_into().unwrap()) as usize + 8,
                    wav.len()
                );
                let fmt = wav.windows(4).position(|chunk| chunk == b"fmt ").unwrap() + 8;
                assert_eq!(&wav[fmt..fmt + 2], &1_u16.to_le_bytes());
                assert_eq!(&wav[fmt + 2..fmt + 4], &1_u16.to_le_bytes());
                assert_eq!(&wav[fmt + 4..fmt + 8], &16000_u32.to_le_bytes());
                assert!((wav.len() as u64) < CONVERTED_AUDIO_LIMIT);
            }
            assert!(matches!(
                convert_native_audio(
                    &original,
                    format,
                    Path::new("ffmpeg"),
                    128,
                    AUDIO_CONVERSION_TIMEOUT
                ),
                Err(TranscriptionError::ConvertedAudioTooLarge { max_bytes: 128 })
            ));
            assert!(matches!(
                convert_native_audio(
                    b"not audio",
                    format,
                    Path::new("ffmpeg"),
                    CONVERTED_AUDIO_LIMIT,
                    AUDIO_CONVERSION_TIMEOUT
                ),
                Err(TranscriptionError::AudioConversionFailed)
            ));
        }
    }
}
