//! Pure utility functions for Telegram payload handling: message chunking,
//! parse-mode normalization, emoji detection, MIME guessing, and filename
//! sanitization. No I/O — extracted out of `telegram.rs` so the long-poll
//! module stays focused on transport plumbing.

use std::path::{Path, PathBuf};

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use serde_json::json;

/// Map a Telegram parse-mode hint to the API value, returning `None` for
/// "no formatting".
pub fn telegram_parse_mode(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "markdown" | "markdownv2" => Some("MarkdownV2"),
        "html" => Some("HTML"),
        _ => None,
    }
}

/// Escape the three characters Telegram's HTML parse-mode treats specially.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Convert the model's GitHub-flavored markdown into the small HTML subset
/// Telegram understands (`<b> <i> <s> <code> <pre> <a> <blockquote>`), sent
/// with `parse_mode=HTML`. This is what makes `**bold**`, lists, links, and
/// code actually render — Telegram's legacy `Markdown` mode only handles
/// single-`*` and chokes on GitHub markdown, falling back to literal text.
///
/// Tables are intentionally not enabled: Telegram HTML has no `<table>`, so
/// they degrade to plain text (and the prompt tells the model to avoid them).
pub fn markdown_to_telegram_html(md: &str) -> String {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    let parser = Parser::new_ext(md, options);

    let mut out = String::new();
    // Stack of ordered-list counters: Some(n) = ordered (next index), None = bullet.
    let mut lists: Vec<Option<u64>> = Vec::new();
    let mut in_code_block = false;

    for event in parser {
        match event {
            Event::Start(Tag::Strong) => out.push_str("<b>"),
            Event::End(TagEnd::Strong) => out.push_str("</b>"),
            Event::Start(Tag::Emphasis) => out.push_str("<i>"),
            Event::End(TagEnd::Emphasis) => out.push_str("</i>"),
            Event::Start(Tag::Strikethrough) => out.push_str("<s>"),
            Event::End(TagEnd::Strikethrough) => out.push_str("</s>"),
            // Telegram has no headings — render them as a bold line.
            Event::Start(Tag::Heading { .. }) => out.push_str("<b>"),
            Event::End(TagEnd::Heading(_)) => out.push_str("</b>\n"),
            Event::Start(Tag::Paragraph) => {}
            Event::End(TagEnd::Paragraph) => out.push_str("\n\n"),
            Event::Start(Tag::CodeBlock(_)) => {
                out.push_str("<pre>");
                in_code_block = true;
            }
            Event::End(TagEnd::CodeBlock) => {
                out.push_str("</pre>\n");
                in_code_block = false;
            }
            Event::Start(Tag::List(start)) => lists.push(start),
            Event::End(TagEnd::List(_)) => {
                lists.pop();
                out.push('\n');
            }
            Event::Start(Tag::Item) => {
                let depth = lists.len().saturating_sub(1);
                out.push_str(&"  ".repeat(depth));
                match lists.last_mut() {
                    Some(Some(n)) => {
                        out.push_str(&format!("{n}. "));
                        *n += 1;
                    }
                    _ => out.push_str("• "),
                }
            }
            Event::End(TagEnd::Item) => out.push('\n'),
            Event::Start(Tag::BlockQuote(_)) => out.push_str("<blockquote>"),
            Event::End(TagEnd::BlockQuote(_)) => out.push_str("</blockquote>\n"),
            Event::Start(Tag::Link { dest_url, .. }) => {
                let href = html_escape(&dest_url).replace('"', "&quot;");
                out.push_str(&format!("<a href=\"{href}\">"));
            }
            Event::End(TagEnd::Link) => out.push_str("</a>"),
            Event::Code(code) => {
                out.push_str(&format!("<code>{}</code>", html_escape(&code)));
            }
            Event::Text(text) => out.push_str(&html_escape(&text)),
            Event::SoftBreak => out.push(if in_code_block { '\n' } else { ' ' }),
            Event::HardBreak => out.push('\n'),
            Event::Rule => out.push('\n'),
            _ => {}
        }
    }

    // Collapse 3+ newlines to a paragraph gap, trim edges.
    let mut collapsed = String::with_capacity(out.len());
    let mut newlines = 0;
    for ch in out.chars() {
        if ch == '\n' {
            newlines += 1;
            if newlines <= 2 {
                collapsed.push('\n');
            }
        } else {
            newlines = 0;
            collapsed.push(ch);
        }
    }
    collapsed.trim().to_string()
}

/// Guess a MIME type from a Telegram file path's extension. Defaults to JPEG
/// because Telegram photo files frequently lack a real extension.
pub fn image_mime_type_from_path(file_path: &str) -> &'static str {
    match file_path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "jpg" | "jpeg" => "image/jpeg",
        _ => "image/jpeg",
    }
}

pub(super) fn image_extension_for_mime(content_type: &str) -> Option<&'static str> {
    match content_type.trim().to_ascii_lowercase().as_str() {
        "image/png" => Some("png"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "image/jpeg" | "image/jpg" => Some("jpg"),
        _ => None,
    }
}

/// True when the entire reply is one or more emoji (possibly with skin-tone
/// modifiers and ZWJ joiners), used to decide whether to react vs message.
pub fn is_emoji_only_reply(text: &str) -> bool {
    let stripped = text.trim();
    if stripped.is_empty() {
        return false;
    }
    let mut saw_emoji = false;
    for ch in stripped.chars() {
        if ch.is_whitespace() || ch == '\u{200d}' || ch == '\u{fe0f}' {
            continue;
        }
        let code = ch as u32;
        if (0x1F3FB..=0x1F3FF).contains(&code) {
            continue;
        }
        if is_emoji_base_char(code) {
            saw_emoji = true;
            continue;
        }
        return false;
    }
    saw_emoji
}

fn is_emoji_base_char(code: u32) -> bool {
    (0x1F1E6..=0x1F1FF).contains(&code)
        || (0x1F300..=0x1FAFF).contains(&code)
        || (0x2600..=0x27BF).contains(&code)
}

pub(super) fn is_invalid_reaction_error(message: &str) -> bool {
    message.to_ascii_uppercase().contains("REACTION_INVALID")
}

pub(super) fn filename_from_url(url: &str) -> String {
    let without_query = url.split('?').next().unwrap_or(url);
    without_query
        .rsplit('/')
        .next()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("file")
        .to_string()
}

pub(super) fn safe_file_name(raw: &str) -> String {
    Path::new(raw)
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty() && *name != "." && *name != "..")
        .unwrap_or("telegram_document")
        .to_string()
}

pub(super) fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(path)
}

pub(super) fn error_payload(message: &str) -> String {
    serde_json::to_string_pretty(&json!({
        "success": false,
        "error": message,
    }))
    .unwrap()
}

/// Chunk a long message into Telegram-sized (4096-char) pieces while
/// respecting paragraph and code-block boundaries.
pub fn split_telegram_messages(text: &str) -> Vec<String> {
    const LIMIT: usize = 4096;
    let mut chunks = Vec::new();
    for segment in telegram_message_segments(text) {
        let mut current = String::new();
        for line in segment.lines() {
            let additional = if current.is_empty() {
                line.len()
            } else {
                line.len() + 1
            };
            if !current.is_empty() && current.len() + additional > LIMIT {
                chunks.push(current);
                current = String::new();
            }
            if line.len() > LIMIT {
                if !current.is_empty() {
                    chunks.push(std::mem::take(&mut current));
                }
                let mut part = String::new();
                for ch in line.chars() {
                    if part.len() + ch.len_utf8() > LIMIT {
                        chunks.push(part);
                        part = String::new();
                    }
                    part.push(ch);
                }
                if !part.is_empty() {
                    chunks.push(part);
                }
            } else {
                if !current.is_empty() {
                    current.push('\n');
                }
                current.push_str(line);
            }
        }
        if !current.trim().is_empty() {
            chunks.push(current);
        }
    }
    if chunks.is_empty() {
        Vec::new()
    } else {
        chunks
    }
}

/// Split on `---` lines, matching Python `main`'s telegram send_message
/// (`segments = [s.strip() for s in text.split("---") if s.strip()]`).
/// We refine slightly: split only on `---` lines that sit OUTSIDE fenced
/// code blocks, so a literal `---` inside a code sample doesn't shatter
/// the message. Lines containing `---` inline (e.g. inside a markdown
/// table separator like `|---|---|`) are also preserved as-is — only
/// pure `---`/`-----` lines act as bubble dividers.
fn telegram_message_segments(text: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    let mut in_code_block = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_code_block = !in_code_block;
            current.push(line);
            continue;
        }
        if !in_code_block && is_divider_line(line) {
            push_segment(&mut segments, &mut current);
            continue;
        }
        current.push(line);
    }
    push_segment(&mut segments, &mut current);
    if segments.is_empty() && !text.trim().is_empty() {
        segments.push(text.trim().to_string());
    }
    segments
}

fn is_divider_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.len() >= 3 && trimmed.chars().all(|c| c == '-')
}

fn push_segment(out: &mut Vec<String>, buffer: &mut Vec<&str>) {
    let joined = buffer.join("\n").trim().to_string();
    if !joined.is_empty() {
        out.push(joined);
    }
    buffer.clear();
}

#[cfg(test)]
mod telegram_html_tests {
    use super::markdown_to_telegram_html as h;

    #[test]
    fn bold_italic_code_render() {
        assert_eq!(h("**bold**"), "<b>bold</b>");
        assert_eq!(h("*it*"), "<i>it</i>");
        assert_eq!(h("`x`"), "<code>x</code>");
    }

    #[test]
    fn headings_become_bold() {
        assert_eq!(h("# Title"), "<b>Title</b>");
    }

    #[test]
    fn links_render_as_anchor() {
        assert_eq!(h("[t](http://x.dev)"), "<a href=\"http://x.dev\">t</a>");
    }

    #[test]
    fn bullet_and_ordered_lists() {
        assert_eq!(h("- a\n- b"), "• a\n• b");
        assert_eq!(h("1. a\n2. b"), "1. a\n2. b");
    }

    #[test]
    fn special_chars_escaped() {
        assert_eq!(h("a < b & c > d"), "a &lt; b &amp; c &gt; d");
    }

    #[test]
    fn code_block_uses_pre() {
        let out = h("```\nlet x = 1 < 2;\n```");
        assert!(out.starts_with("<pre>"));
        assert!(out.contains("1 &lt; 2"));
        assert!(out.contains("</pre>"));
    }

    #[test]
    fn tables_degrade_to_text_not_html() {
        // Tables aren't enabled → no <table>; pipes survive as literal text.
        let out = h("| a | b |\n|---|---|\n| 1 | 2 |");
        assert!(!out.contains("<table>"));
        assert!(out.contains('|'));
    }
}
