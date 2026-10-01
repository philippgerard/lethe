//! Pure Markdown rendering for Linq text parts and their UTF-16 decorations.

use std::ops::Range;

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DecorationStyle {
    Bold,
    Italic,
    Strikethrough,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TextDecoration {
    /// Half-open UTF-16 offsets, as required by Linq; these are not byte offsets.
    pub range: [usize; 2],
    pub style: DecorationStyle,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenderedText {
    pub text: String,
    pub text_decorations: Vec<TextDecoration>,
}

impl RenderedText {
    /// Split on Unicode scalar boundaries and clip/rebase every style range.
    /// Rebalance a blank tail into a chunk with visible content where possible.
    /// Linq forbids whitespace-only parts: if a whitespace run cannot fit next
    /// to content within the limit, omit only the otherwise standalone excess.
    pub fn chunks(&self, limit_chars: usize) -> Vec<Self> {
        let limit_chars = limit_chars.max(1);
        let characters = self.text.chars().collect::<Vec<_>>();
        let mut offsets = Vec::with_capacity(characters.len() + 1);
        let mut byte_offset = 0;
        let mut utf16_offset = 0;
        offsets.push((byte_offset, utf16_offset));
        for character in &characters {
            byte_offset += character.len_utf8();
            utf16_offset += character.len_utf16();
            offsets.push((byte_offset, utf16_offset));
        }
        // Count legal suffix partitions. A chunk must reach the next content
        // character and end at a suffix that can itself be partitioned. Range
        // counts keep this linear even with the normal 3000-character limit.
        let mut legal_suffixes = vec![0; characters.len() + 2];
        legal_suffixes[characters.len()] = 1;
        let mut next_content = characters.len();
        for start in (0..characters.len()).rev() {
            if !characters[start].is_whitespace() {
                next_content = start;
            }
            let first_end = next_content + 1;
            let last_end = start.saturating_add(limit_chars).min(characters.len());
            let possible =
                first_end <= last_end && legal_suffixes[first_end] > legal_suffixes[last_end + 1];
            legal_suffixes[start] = legal_suffixes[start + 1] + usize::from(possible);
        }
        let mut last_legal_end = vec![None; characters.len() + 1];
        let mut previous = None;
        for end in 0..=characters.len() {
            if legal_suffixes[end] > legal_suffixes[end + 1] {
                previous = Some(end);
            }
            last_legal_end[end] = previous;
        }
        let mut chunks = Vec::new();
        let mut start = 0;
        while start < characters.len() {
            let mut end = start.saturating_add(limit_chars).min(characters.len());
            let preserve_suffix = legal_suffixes[start] > legal_suffixes[start + 1];
            if preserve_suffix {
                end = last_legal_end[end].expect("a legal suffix has a legal chunk end");
            }
            let last_content = characters[start..end]
                .iter()
                .rposition(|character| !character.is_whitespace())
                .map(|offset| start + offset);
            let Some(last_content) = last_content else {
                let Some(next_content) = characters[end..]
                    .iter()
                    .position(|character| !character.is_whitespace())
                    .map(|offset| end + offset)
                else {
                    break;
                };
                // Keep as much of the leading whitespace as fits alongside
                // the next content character, rather than sending a blank part.
                start = next_content.saturating_sub(limit_chars - 1);
                continue;
            };
            let next_end = end.saturating_add(limit_chars).min(characters.len());
            if !preserve_suffix
                && end < characters.len()
                && characters[end..next_end]
                    .iter()
                    .all(|ch| ch.is_whitespace())
                && characters[start..last_content]
                    .iter()
                    .any(|ch| !ch.is_whitespace())
            {
                // Leave the final content character with the following blank
                // tail. Both resulting chunks still contain visible content.
                end = last_content;
            }
            let (byte_start, utf16_start) = offsets[start];
            let (byte_end, utf16_end) = offsets[end];
            chunks.push(self.chunk(byte_start..byte_end, utf16_start..utf16_end));
            start = end;
        }
        chunks
    }

    fn chunk(&self, bytes: Range<usize>, utf16: Range<usize>) -> Self {
        let text_decorations = self
            .text_decorations
            .iter()
            .filter_map(|decoration| {
                let start = decoration.range[0].max(utf16.start);
                let end = decoration.range[1].min(utf16.end);
                (start < end).then_some(TextDecoration {
                    range: [start - utf16.start, end.saturating_sub(utf16.start)],
                    style: decoration.style,
                })
            })
            .collect();
        Self {
            text: self.text[bytes].to_string(),
            text_decorations,
        }
    }
}

/// Render parsed formatting without scrubbing literal Markdown characters.
/// Links retain their destinations; code retains its content. Struck-out text
/// has an explicit plain-text label because SMS/RCS may ignore decorations.
pub fn render_imessage_markdown(markdown: &str) -> RenderedText {
    // Math is parsed solely to protect its literal source from emphasis rules.
    // Tables remain literal text, so their cells and separators are not lost.
    let options = Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS | Options::ENABLE_MATH;
    let mut writer = TextWriter::default();
    let mut lists: Vec<Option<u64>> = Vec::new();
    let mut styles: Vec<(DecorationStyle, usize)> = Vec::new();
    let mut decorations = Vec::new();
    let mut links: Vec<(String, usize)> = Vec::new();
    let mut in_code_block = false;
    let mut skipped_depth = 0;

    for (event, range) in Parser::new_ext(markdown, options).into_offset_iter() {
        if skipped_depth > 0 {
            match event {
                Event::Start(_) => skipped_depth += 1,
                Event::End(_) => skipped_depth -= 1,
                _ => {}
            }
            continue;
        }
        match event {
            Event::Start(Tag::Emphasis | Tag::Strong)
                if embedded_technical_token(markdown, &range) =>
            {
                writer.push(&markdown[range]);
                skipped_depth = 1;
            }
            Event::Start(Tag::Strong) => {
                writer.flush_break();
                styles.push((DecorationStyle::Bold, writer.utf16_len));
            }
            Event::Start(Tag::Emphasis) => {
                writer.flush_break();
                styles.push((DecorationStyle::Italic, writer.utf16_len));
            }
            Event::Start(Tag::Strikethrough) => {
                writer.push("[struck out: ");
                styles.push((DecorationStyle::Strikethrough, writer.utf16_len));
            }
            Event::End(TagEnd::Strong | TagEnd::Emphasis) => {
                end_style(&writer, &mut styles, &mut decorations);
            }
            Event::End(TagEnd::Strikethrough) => {
                end_style(&writer, &mut styles, &mut decorations);
                writer.push("]");
            }
            Event::Start(Tag::Heading { .. }) => {
                writer.line_break(2);
                writer.flush_break();
                styles.push((DecorationStyle::Bold, writer.utf16_len));
            }
            Event::End(TagEnd::Heading(_)) => {
                end_style(&writer, &mut styles, &mut decorations);
                writer.line_break(2);
            }
            Event::End(TagEnd::Paragraph) | Event::Rule => writer.line_break(2),
            Event::Start(Tag::List(start)) => {
                writer.line_break(if lists.is_empty() { 2 } else { 1 });
                lists.push(start);
            }
            Event::End(TagEnd::List(_)) => {
                lists.pop();
                writer.line_break(if lists.is_empty() { 2 } else { 1 });
            }
            Event::Start(Tag::Item) => {
                writer.line_break(1);
                writer.push(&"  ".repeat(lists.len().saturating_sub(1)));
                match lists.last_mut() {
                    Some(Some(number)) => {
                        writer.push(&format!("{number}. "));
                        *number += 1;
                    }
                    _ => writer.push("• "),
                }
            }
            Event::End(TagEnd::Item) => writer.pending_break = 1,
            Event::Start(Tag::BlockQuote(_)) => {
                writer.line_break(2);
                writer.push("> ");
            }
            Event::End(TagEnd::BlockQuote(_)) => writer.line_break(2),
            Event::Start(Tag::CodeBlock(_)) => {
                writer.line_break(2);
                in_code_block = true;
            }
            Event::End(TagEnd::CodeBlock) => {
                in_code_block = false;
                writer.line_break(2);
            }
            Event::Start(Tag::Link { dest_url, .. } | Tag::Image { dest_url, .. }) => {
                writer.flush_break();
                links.push((dest_url.into_string(), writer.text.len()));
            }
            Event::End(TagEnd::Link | TagEnd::Image) => {
                if let Some((destination, label_start)) = links.pop() {
                    let label = writer.text[label_start..].trim();
                    if label.is_empty() {
                        writer.push(&destination);
                    } else if label != destination
                        && destination.strip_prefix("mailto:") != Some(label)
                    {
                        writer.push(&format!(" ({destination})"));
                    }
                }
            }
            Event::Code(_) => {
                // pulldown normalizes code-span whitespace; use source offsets
                // so commands and arguments keep their exact literal content.
                let source = &markdown[range];
                let delimiter_len = source.bytes().take_while(|byte| *byte == b'`').count();
                writer.push(&source[delimiter_len..source.len() - delimiter_len]);
            }
            Event::InlineMath(_) | Event::DisplayMath(_) => writer.push(&markdown[range]),
            Event::Text(text) => {
                if in_code_block
                    && text.as_ref() == "\n"
                    && range.start > 0
                    && markdown.as_bytes()[range.start - 1] == b'\r'
                {
                    writer.push("\r\n");
                } else {
                    writer.push(&text);
                }
            }
            Event::Html(html) | Event::InlineHtml(html) => writer.push(&html),
            Event::SoftBreak | Event::HardBreak => writer.push("\n"),
            Event::TaskListMarker(checked) => writer.push(if checked { "[x] " } else { "[ ] " }),
            Event::FootnoteReference(label) => writer.push(&format!("[^{label}]")),
            _ => {}
        }
    }
    decorations.sort_by_key(|decoration| (decoration.range, decoration.style));
    decorations.dedup();
    RenderedText {
        text: writer.text,
        text_decorations: decorations,
    }
}

fn end_style(
    writer: &TextWriter,
    styles: &mut Vec<(DecorationStyle, usize)>,
    decorations: &mut Vec<TextDecoration>,
) {
    if let Some((style, start)) = styles.pop()
        && start < writer.utf16_len
    {
        decorations.push(TextDecoration {
            range: [start, writer.utf16_len],
            style,
        });
    }
}

/// CommonMark can mistake stars in `src/*/test*.rs` or `--glob=*foo*`
/// for emphasis. Preserve a range embedded in a larger technical token.
fn embedded_technical_token(markdown: &str, range: &Range<usize>) -> bool {
    // A link's destination is a separate token from its label; its slashes
    // must not make emphasis in `[**label**](https://...)` look like a path.
    let boundary =
        |character: char| character.is_whitespace() || matches!(character, '[' | ']' | '<' | '>');
    let token_start = markdown[..range.start].rfind(boundary).map_or(0, |offset| {
        offset + markdown[offset..].chars().next().unwrap().len_utf8()
    });
    let token_end = markdown[range.end..]
        .find(boundary)
        .map_or(markdown.len(), |offset| range.end + offset);
    if token_start == range.start && token_end == range.end {
        return false;
    }
    let before = &markdown[token_start..range.start];
    let after = &markdown[range.end..token_end];
    let token = &markdown[token_start..token_end];
    before.contains(['/', '\\'])
        || after.contains(['/', '\\'])
        || (token.starts_with('-') && token.contains('='))
        || after
            .strip_prefix('.')
            .and_then(|extension| extension.chars().next())
            .is_some_and(|character| character.is_ascii_alphanumeric())
}

#[derive(Default)]
struct TextWriter {
    text: String,
    utf16_len: usize,
    pending_break: usize,
}

impl TextWriter {
    fn line_break(&mut self, count: usize) {
        self.pending_break = self.pending_break.max(count);
    }

    fn flush_break(&mut self) {
        if !self.text.is_empty() {
            let trailing = self.text.chars().rev().take_while(|ch| *ch == '\n').count();
            for _ in trailing..self.pending_break {
                self.text.push('\n');
                self.utf16_len += 1;
            }
        }
        self.pending_break = 0;
    }

    fn push(&mut self, text: &str) {
        if !text.is_empty() {
            self.flush_break();
            self.text.push_str(text);
            self.utf16_len += text.encode_utf16().count();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoration(range: [usize; 2], style: DecorationStyle) -> TextDecoration {
        TextDecoration { range, style }
    }

    #[test]
    fn headings_emphasis_lists_and_rules_render_without_markdown_markers() {
        let rendered = render_imessage_markdown(
            "# Next steps\n\n**Ready** and *waiting*.\n\n---\n\n- First\n  - Nested\n- Second\n\n3. Third\n4. Fourth",
        );
        assert_eq!(
            rendered.text,
            "Next steps\n\nReady and waiting.\n\n• First\n  • Nested\n• Second\n\n3. Third\n4. Fourth"
        );
        assert_eq!(
            rendered.text_decorations,
            vec![
                decoration([0, 10], DecorationStyle::Bold),
                decoration([12, 17], DecorationStyle::Bold),
                decoration([22, 29], DecorationStyle::Italic),
            ]
        );
    }

    #[test]
    fn links_images_references_and_autolinks_keep_destinations() {
        let rendered = render_imessage_markdown(
            "[**Details**](https://example.com/a?q=x_y&total=29.95)\n\n[Terms][t]\n\n[t]: https://example.com/terms\n\n<https://example.com> <user@example.com>\n\n![receipt](https://example.com/receipt.png)",
        );
        assert_eq!(
            rendered.text,
            "Details (https://example.com/a?q=x_y&total=29.95)\n\nTerms (https://example.com/terms)\n\nhttps://example.com user@example.com\n\nreceipt (https://example.com/receipt.png)"
        );
        assert_eq!(
            rendered.text_decorations[0],
            decoration([0, 7], DecorationStyle::Bold)
        );
    }

    #[test]
    fn code_preserves_spaces_newlines_markers_and_literal_arguments() {
        let code = "  lethe run --id=req_123 --glob='**/*.rs'\n\n\n  echo '**€29.95**'  \n";
        let rendered = render_imessage_markdown(&format!("```sh\n{code}```"));
        assert_eq!(rendered.text, code);
        assert!(rendered.text_decorations.is_empty());
        assert_eq!(render_imessage_markdown("`  a\n b  `").text, "  a\n b  ");
        assert_eq!(render_imessage_markdown("``a ` b``").text, "a ` b");
        assert_eq!(render_imessage_markdown("`   `").text, "   ");
        assert_eq!(render_imessage_markdown("```\r\n a\r\n```").text, " a\r\n");
    }

    #[test]
    fn technical_tokens_math_and_unmatched_markers_stay_literal() {
        let text = "foo_bar_baz a_b **/*.rs *.rs and *.md src/*/test*.rs src/*foo*/bar --glob=*foo* *foo*.rs 2 * 3 * 4 $a_b * c_d$ **unfinished";
        let rendered = render_imessage_markdown(text);
        assert_eq!(rendered.text, text);
        assert!(rendered.text_decorations.is_empty());
        let math = "$$\nx_i ** y_i\n$$";
        assert_eq!(render_imessage_markdown(math).text, math);
    }

    #[test]
    fn approval_terms_ids_and_commands_survive_formatting() {
        let rendered = render_imessage_markdown(
            "**Approve €29.95**, then run `/approve 123e4567-e89b-12d3-a456-426614174000`.\n\nAccount: customer_123\nArguments: `--limit=29.95 --owner=customer_123 --path=src/*/test*.rs`",
        );
        assert_eq!(
            rendered.text,
            "Approve €29.95, then run /approve 123e4567-e89b-12d3-a456-426614174000.\n\nAccount: customer_123\nArguments: --limit=29.95 --owner=customer_123 --path=src/*/test*.rs"
        );
    }

    #[test]
    fn strikethrough_remains_unambiguous_without_native_styles() {
        let rendered = render_imessage_markdown("Pay ~~€50~~ **€45**, not ~~`--limit=50`~~.");
        assert_eq!(
            rendered.text,
            "Pay [struck out: €50] €45, not [struck out: --limit=50]."
        );
        let strike = rendered
            .text_decorations
            .iter()
            .filter(|span| span.style == DecorationStyle::Strikethrough)
            .collect::<Vec<_>>();
        assert_eq!(strike.len(), 2);
        let units = rendered.text.encode_utf16().collect::<Vec<_>>();
        assert_eq!(
            String::from_utf16(&units[strike[0].range[0]..strike[0].range[1]]).unwrap(),
            "€50"
        );
        assert_eq!(
            String::from_utf16(&units[strike[1].range[0]..strike[1].range[1]]).unwrap(),
            "--limit=50"
        );
    }

    #[test]
    fn overlapping_styles_and_astral_emoji_use_utf16_offsets() {
        let rendered = render_imessage_markdown("🙂 **A🚀*B*Z** end");
        assert_eq!(rendered.text, "🙂 A🚀BZ end");
        assert_eq!(
            rendered.text_decorations,
            vec![
                decoration([3, 8], DecorationStyle::Bold),
                decoration([6, 7], DecorationStyle::Italic)
            ]
        );
        assert_eq!(
            serde_json::to_value(&rendered.text_decorations).unwrap(),
            serde_json::json!([
                {"range": [3, 8], "style": "bold"}, {"range": [6, 7], "style": "italic"}
            ])
        );
    }

    #[test]
    fn chunk_crossing_clips_styles_and_preserves_every_character() {
        let rendered = render_imessage_markdown("🙂 **A🚀*B*Z** end");
        let chunks = rendered.chunks(4);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<Vec<_>>(),
            vec!["🙂 A🚀", "BZ e", "nd"]
        );
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<String>(),
            rendered.text
        );
        assert_eq!(
            chunks[0].text_decorations,
            vec![decoration([3, 6], DecorationStyle::Bold)]
        );
        assert_eq!(
            chunks[1].text_decorations,
            vec![
                decoration([0, 2], DecorationStyle::Bold),
                decoration([0, 1], DecorationStyle::Italic)
            ]
        );
        assert!(chunks[2].text_decorations.is_empty());
        for chunk in &chunks {
            let units = chunk.text.encode_utf16().collect::<Vec<_>>();
            for span in &chunk.text_decorations {
                assert!(span.range[0] < span.range[1]);
                assert!(span.range[1] <= units.len());
                assert!(String::from_utf16(&units[span.range[0]..span.range[1]]).is_ok());
            }
        }
    }

    #[test]
    fn terminal_code_newline_is_rebalanced_without_losing_content() {
        let content = format!("{}\n", "x".repeat(3000));
        let rendered = render_imessage_markdown(&format!("```\n{content}```"));
        let chunks = rendered.chunks(3000);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "x".repeat(2999));
        assert_eq!(chunks[1].text, "x\n");
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<String>(),
            content
        );
    }

    #[test]
    fn rebalanced_emoji_chunk_rebases_overlapping_utf16_styles() {
        let rendered = RenderedText {
            text: format!("{}🙂\n", "a".repeat(2999)),
            text_decorations: vec![
                decoration([0, 3001], DecorationStyle::Bold),
                decoration([2999, 3001], DecorationStyle::Italic),
            ],
        };
        let chunks = rendered.chunks(3000);
        assert_eq!(chunks[0].text, "a".repeat(2999));
        assert_eq!(chunks[1].text, "🙂\n");
        assert_eq!(
            chunks[0].text_decorations,
            vec![decoration([0, 2999], DecorationStyle::Bold)]
        );
        assert_eq!(
            chunks[1].text_decorations,
            vec![
                decoration([0, 2], DecorationStyle::Bold),
                decoration([0, 2], DecorationStyle::Italic),
            ]
        );
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<String>(),
            rendered.text
        );
    }

    #[test]
    fn blank_chunks_are_omitted_only_when_the_limit_prevents_rebalancing() {
        assert!(render_imessage_markdown("` \t\n `").chunks(3000).is_empty());
        assert!(
            render_imessage_markdown("```\n\n\n```")
                .chunks(3000)
                .is_empty()
        );

        let representable = render_imessage_markdown("`abcdefgh    🙂`");
        let chunks = representable.chunks(4);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<String>(),
            representable.text
        );
        assert!(chunks.iter().all(|chunk| !chunk.text.trim().is_empty()));

        let text = format!("a{}🚀{}", " ".repeat(12), " ".repeat(12));
        let pathological = RenderedText {
            text: text.clone(),
            text_decorations: vec![decoration(
                [0, text.encode_utf16().count()],
                DecorationStyle::Bold,
            )],
        };
        let chunks = pathological.chunks(4);
        let transmitted = chunks
            .iter()
            .map(|chunk| chunk.text.as_str())
            .collect::<String>();
        assert_eq!(
            transmitted
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>(),
            "a🚀"
        );
        assert!(transmitted.len() < pathological.text.len());
        for chunk in chunks {
            assert!(!chunk.text.trim().is_empty());
            assert!(chunk.text.chars().count() <= 4);
            let span = &chunk.text_decorations[0];
            assert_eq!(span.range, [0, chunk.text.encode_utf16().count()]);
        }
    }

    #[test]
    fn short_whitespace_patterns_preserve_all_representable_content() {
        // Independently check whether any legal partition exists, so chunk
        // rebalancing cannot silently discard whitespace when preservation
        // within the provider's nonempty-part constraint is possible.
        for length in 0..=9 {
            for mask in 0..(1 << length) {
                let characters = (0..length)
                    .map(|index| {
                        if mask & (1 << index) == 0 {
                            ' '
                        } else if index % 2 == 0 {
                            '🙂'
                        } else {
                            'x'
                        }
                    })
                    .collect::<Vec<_>>();
                let text = characters.iter().collect::<String>();
                let rendered = RenderedText {
                    text: text.clone(),
                    text_decorations: vec![decoration(
                        [0, text.encode_utf16().count()],
                        DecorationStyle::Bold,
                    )],
                };
                for limit in 1..=5 {
                    let mut possible = vec![false; length + 1];
                    possible[0] = true;
                    for end in 1..=length {
                        possible[end] = (end.saturating_sub(limit)..end).any(|start| {
                            possible[start]
                                && characters[start..end].iter().any(|ch| !ch.is_whitespace())
                        });
                    }
                    let chunks = rendered.chunks(limit);
                    let transmitted = chunks
                        .iter()
                        .map(|chunk| chunk.text.as_str())
                        .collect::<String>();
                    if possible[length] {
                        assert_eq!(transmitted, text, "text={text:?}, limit={limit}");
                    }
                    assert_eq!(
                        transmitted
                            .chars()
                            .filter(|ch| !ch.is_whitespace())
                            .collect::<String>(),
                        text.chars()
                            .filter(|ch| !ch.is_whitespace())
                            .collect::<String>()
                    );
                    for chunk in chunks {
                        assert!(!chunk.text.trim().is_empty());
                        assert!(chunk.text.chars().count() <= limit);
                        let units = chunk.text.encode_utf16().collect::<Vec<_>>();
                        assert_eq!(chunk.text_decorations[0].range, [0, units.len()]);
                    }
                }
            }
        }
    }

    #[test]
    fn task_status_tables_html_and_plain_text_are_not_deleted() {
        assert_eq!(
            render_imessage_markdown("- [x] done\n- [ ] pending").text,
            "• [x] done\n• [ ] pending"
        );
        let table = "| Amount | ID |\n|---|---|\n| €29.95 | customer_123 |";
        assert_eq!(render_imessage_markdown(table).text, table);
        let literal = "a < b & c > d\n<span>keep</span>";
        assert_eq!(render_imessage_markdown(literal).text, literal);
        assert!(render_imessage_markdown("").chunks(3000).is_empty());
        assert_eq!(
            render_imessage_markdown("a🙂")
                .chunks(0)
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "🙂"]
        );
    }
}
