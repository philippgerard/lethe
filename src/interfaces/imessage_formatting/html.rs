//! Explicit HTML input is rendered directly, without interpreting its text as
//! Markdown. The HTML5 tokenizer handles quoted attributes, entities, malformed
//! input and raw-text elements; a small stack maps formatting to Linq ranges.

use std::cell::RefCell;

use html5ever::tokenizer::{
    BufferQueue, Tag, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, states::RawKind,
};

use super::{DecorationStyle, RenderedText, TextDecoration, TextWriter};

/// Render Telegram-style HTML into readable text and native iMessage styles.
/// Unsupported inline styles retain their text. Links retain destinations, and
/// struck-out text keeps the same explicit SMS/RCS fallback as Markdown.
pub fn render_imessage_html(html: &str) -> RenderedText {
    let input = BufferQueue::default();
    input.push_back(html.into());
    let tokenizer = Tokenizer::new(HtmlSink::default(), Default::default());
    let _ = tokenizer.feed(&input);
    tokenizer.end();
    tokenizer.sink.0.into_inner().finish()
}

#[derive(Default)]
struct HtmlSink(RefCell<HtmlWriter>);

impl TokenSink for HtmlSink {
    type Handle = ();

    fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
        let mut writer = self.0.borrow_mut();
        match token {
            Token::CharacterTokens(text) if writer.hidden_depth == 0 => writer.text.push(&text),
            Token::NullCharacterToken if writer.hidden_depth == 0 => writer.text.push("\u{fffd}"),
            Token::TagToken(tag) => match tag.kind {
                TagKind::StartTag => return writer.start(tag),
                TagKind::EndTag => writer.close(tag.name.as_ref()),
            },
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

#[derive(Default)]
struct HtmlWriter {
    text: TextWriter,
    elements: Vec<Element>,
    decorations: Vec<TextDecoration>,
    lists: Vec<Option<u64>>,
    hidden_depth: usize,
}

struct Element {
    name: String,
    style: Option<(DecorationStyle, usize)>,
    link: Option<(String, usize)>,
    hidden: bool,
}

impl HtmlWriter {
    fn start(&mut self, tag: Tag) -> TokenSinkResult<()> {
        let name = tag.name.as_ref();
        let hidden = self.hidden_depth > 0 || matches!(name, "script" | "style" | "head");
        let mut element = Element {
            name: name.to_string(),
            style: None,
            link: None,
            hidden,
        };
        if hidden {
            self.hidden_depth += 1;
        } else {
            match name {
                "br" => self.text.push("\n"),
                "hr" => self.text.line_break(2),
                "ul" | "ol" => {
                    self.text
                        .line_break(if self.lists.is_empty() { 2 } else { 1 });
                    self.lists.push((name == "ol").then(|| {
                        attribute(&tag, "start")
                            .and_then(|start| start.parse().ok())
                            .unwrap_or(1)
                    }));
                }
                "li" => {
                    self.text.line_break(1);
                    self.text
                        .push(&"  ".repeat(self.lists.len().saturating_sub(1)));
                    match self.lists.last_mut() {
                        Some(Some(number)) => {
                            self.text.push(&format!("{number}. "));
                            *number = number.saturating_add(1);
                        }
                        _ => self.text.push("• "),
                    }
                }
                "td" | "th" => {
                    if self.text.pending_break == 0 && !self.text.text.is_empty() {
                        self.text.push(" | ");
                    }
                }
                "img" => {
                    if let Some(alt) = attribute(&tag, "alt") {
                        self.text.push(alt);
                    }
                }
                _ => self.text.line_break(block_break(name)),
            }
            let style = match name {
                "b" | "strong" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                    Some(DecorationStyle::Bold)
                }
                "i" | "em" => Some(DecorationStyle::Italic),
                "s" | "strike" | "del" => {
                    self.text.push("[struck out: ");
                    Some(DecorationStyle::Strikethrough)
                }
                _ => None,
            };
            if let Some(style) = style {
                self.text.flush_break();
                element.style = Some((style, self.text.utf16_len));
            }
            if name == "a"
                && let Some(destination) = attribute(&tag, "href").filter(|value| !value.is_empty())
            {
                self.text.flush_break();
                element.link = Some((destination.to_string(), self.text.text.len()));
            }
        }

        // HTML's void elements have no end tag. Honor explicit self-closing
        // tags too, avoiding leaked styles from producer-generated fragments.
        if tag.self_closing || is_void(name) {
            self.end(element);
        } else {
            self.elements.push(element);
        }
        match name {
            "script" if !tag.self_closing => TokenSinkResult::RawData(RawKind::ScriptData),
            "style" | "xmp" | "iframe" | "noembed" | "noframes" if !tag.self_closing => {
                TokenSinkResult::RawData(RawKind::Rawtext)
            }
            "title" | "textarea" if !tag.self_closing => TokenSinkResult::RawData(RawKind::Rcdata),
            "plaintext" if !tag.self_closing => TokenSinkResult::Plaintext,
            _ => TokenSinkResult::Continue,
        }
    }

    fn close(&mut self, name: &str) {
        if let Some(index) = self
            .elements
            .iter()
            .rposition(|element| element.name == name)
        {
            // Close dangling inner tags as well, keeping malformed fragments
            // from styling unrelated text after their enclosing element ends.
            while self.elements.len() > index {
                let element = self.elements.pop().unwrap();
                self.end(element);
            }
        }
    }

    fn end(&mut self, element: Element) {
        if element.hidden {
            self.hidden_depth -= 1;
            return;
        }
        if let Some((destination, label_start)) = element.link {
            let label = self.text.text[label_start..].trim();
            if label.is_empty() {
                self.text.push(&destination);
            } else if label != destination && destination.strip_prefix("mailto:") != Some(label) {
                self.text.push(&format!(" ({destination})"));
            }
        }
        if let Some((style, start)) = element.style {
            if start < self.text.utf16_len {
                self.decorations.push(TextDecoration {
                    range: [start, self.text.utf16_len],
                    style,
                });
            }
            if style == DecorationStyle::Strikethrough {
                self.text.push("]");
            }
        }
        match element.name.as_str() {
            "ul" | "ol" => {
                self.lists.pop();
                self.text
                    .line_break(if self.lists.is_empty() { 2 } else { 1 });
            }
            "li" => self.text.line_break(1),
            name => self.text.line_break(block_break(name)),
        }
    }

    fn finish(mut self) -> RenderedText {
        while let Some(element) = self.elements.pop() {
            self.end(element);
        }
        self.decorations
            .sort_by_key(|decoration| (decoration.range, decoration.style));
        self.decorations.dedup();
        RenderedText {
            text: self.text.text,
            text_decorations: self.decorations,
        }
    }
}

fn attribute<'a>(tag: &'a Tag, name: &str) -> Option<&'a str> {
    tag.attrs
        .iter()
        .find(|attribute| &*attribute.name.local == name)
        .map(|attribute| attribute.value.as_ref())
}

fn block_break(name: &str) -> usize {
    match name {
        "p" | "pre" | "blockquote" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "section"
        | "article" | "header" | "footer" | "table" => 2,
        "div" | "tr" => 1,
        _ => 0,
    }
}

fn is_void(name: &str) -> bool {
    matches!(
        name,
        "area"
            | "base"
            | "br"
            | "col"
            | "embed"
            | "hr"
            | "img"
            | "input"
            | "link"
            | "meta"
            | "param"
            | "source"
            | "track"
            | "wbr"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decorated_text(rendered: &RenderedText, decoration: &TextDecoration) -> String {
        let units: Vec<u16> = rendered.text.encode_utf16().collect();
        String::from_utf16(&units[decoration.range[0]..decoration.range[1]]).unwrap()
    }

    #[test]
    fn nutrition_html_renders_native_bold_without_visible_tags() {
        let rendered = render_imessage_html(
            "🥐 <b>Frühstück – Schätzung, bitte ggf. prüfen:</b> Franzbrötchen + Flat White ≈ \
             <b>557 kcal · 12 g Protein</b>.\n\n🎯 <b>Heute bisher im Log</b>: 1.345 / 1.900 kcal",
        );
        assert_eq!(
            rendered.text,
            "🥐 Frühstück – Schätzung, bitte ggf. prüfen: Franzbrötchen + Flat White ≈ \
             557 kcal · 12 g Protein.\n\n🎯 Heute bisher im Log: 1.345 / 1.900 kcal"
        );
        let labels = rendered
            .text_decorations
            .iter()
            .map(|decoration| {
                assert_eq!(decoration.style, DecorationStyle::Bold);
                decorated_text(&rendered, decoration)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            [
                "Frühstück – Schätzung, bitte ggf. prüfen:",
                "557 kcal · 12 g Protein",
                "Heute bisher im Log"
            ]
        );
    }

    #[test]
    fn nested_styles_and_astral_characters_use_utf16_ranges() {
        let rendered = render_imessage_html("🙂 <STRONG>A🚀<em>B</em>Z</STRONG> <del>€50</del>");
        assert_eq!(rendered.text, "🙂 A🚀BZ [struck out: €50]");
        assert_eq!(
            rendered.text_decorations,
            [
                TextDecoration {
                    range: [3, 8],
                    style: DecorationStyle::Bold,
                },
                TextDecoration {
                    range: [6, 7],
                    style: DecorationStyle::Italic,
                },
                TextDecoration {
                    range: [22, 25],
                    style: DecorationStyle::Strikethrough,
                },
            ]
        );
        for alias in ["s", "strike", "del"] {
            let rendered = render_imessage_html(&format!("<{alias}>old</{alias}>"));
            assert_eq!(rendered.text, "[struck out: old]");
            assert_eq!(
                decorated_text(&rendered, &rendered.text_decorations[0]),
                "old"
            );
        }
    }

    #[test]
    fn entities_code_and_markdown_characters_remain_literal() {
        let rendered = render_imessage_html(
            "<p>&lt;b&gt;literal&lt;/b&gt; &amp; &quot;x&quot; &#39;y&#39; &#x1f680; &copy;</p>\
             <pre><code>  **bold?**\n--glob=*foo* &lt;tag&gt;\n`x` &amp;lt; &unknown;  </code></pre>\
             <p>*stars* _underscores_ [label](https://example.test) ~~keep~~</p>",
        );
        assert_eq!(
            rendered.text,
            "<b>literal</b> & \"x\" 'y' 🚀 ©\n\n  **bold?**\n--glob=*foo* <tag>\n\
             `x` &lt; &unknown;  \n\n*stars* _underscores_ [label](https://example.test) ~~keep~~"
        );
        assert!(rendered.text_decorations.is_empty());
    }

    #[test]
    fn links_breaks_and_unsupported_styles_remain_readable() {
        let rendered = render_imessage_html(
            "<p><a href='https://example.test/?x=1&amp;y=2' title='a > b'><b>Details</b></a>\
             <br>Next<br><br><u>underlined</u> <tg-spoiler>spoiler</tg-spoiler></p>\
             <div><a href=https://example.test>https://example.test</a></div>\
             <div><a href='mailto:user@example.test'>user@example.test</a></div>\
             <p><a href='https://empty.test'></a></p>",
        );
        assert_eq!(
            rendered.text,
            "Details (https://example.test/?x=1&y=2)\nNext\n\nunderlined spoiler\n\n\
             https://example.test\nuser@example.test\n\nhttps://empty.test"
        );
        assert_eq!(rendered.text_decorations.len(), 1);
        assert_eq!(
            decorated_text(&rendered, &rendered.text_decorations[0]),
            "Details"
        );
    }

    #[test]
    fn malformed_html_closes_styles_and_hides_nonvisible_elements() {
        let rendered = render_imessage_html(
            "<b>bold <i>nested</b> plain</i><!-- ignored -->\
             <script>if (a < b) alert('<b>hidden</b>')</script>\
             <style>b { color: red; }</style><strong>tail",
        );
        assert_eq!(rendered.text, "bold nested plaintail");
        assert_eq!(
            rendered
                .text_decorations
                .iter()
                .map(|decoration| decorated_text(&rendered, decoration))
                .collect::<Vec<_>>(),
            ["bold nested", "nested", "tail"]
        );
        assert_eq!(
            render_imessage_html("2 < 3 &amp; 5 > 4").text,
            "2 < 3 & 5 > 4"
        );
    }

    #[test]
    fn html_lists_and_tables_keep_item_boundaries() {
        let rendered = render_imessage_html(
            "<ol start='3'><li>First</li><li><b>Second</b></li></ol>\
             <table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>",
        );
        assert_eq!(rendered.text, "3. First\n4. Second\n\nA | B\n1 | 2");
        assert_eq!(
            decorated_text(&rendered, &rendered.text_decorations[0]),
            "Second"
        );
    }

    #[test]
    fn long_html_chunks_preserve_content_and_clip_styles_at_utf16_boundaries() {
        let content = "A🙂".repeat(3200);
        let rendered = render_imessage_html(&format!("<b>{content}</b>"));
        let chunks = rendered.chunks(3000);
        assert_eq!(chunks.len(), 3);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<String>(),
            content
        );
        for chunk in chunks {
            assert!(chunk.text.chars().count() <= 3000);
            assert_eq!(
                chunk.text_decorations,
                [TextDecoration {
                    range: [0, chunk.text.encode_utf16().count()],
                    style: DecorationStyle::Bold,
                }]
            );
        }
    }
}
