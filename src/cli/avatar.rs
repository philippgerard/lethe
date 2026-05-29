//! Animated ASCII avatar shown by `lethe` (status) and `lethe init`.
//!
//! Frames in `avatar_frames.ansi` are pre-rendered: an image-to-video clip of
//! the Lethe avatar (her hair flowing) was generated from `lethe-avatar.png`,
//! then each frame was converted to colored Unicode with `chafa --fg-only`
//! (foreground only, so the terminal's own background shows through — no baked
//! black). See `scripts/gen-avatar-frames`. Frames are form-feed separated.
//!
//! [`play_above`] prints the caller's text first (so output is visible
//! immediately), then animates the avatar *above* it in place for ~1.7s — the
//! text stays on screen the whole time, so the animation runs alongside the
//! output rather than blocking it. No-op unless a color-capable TTY tall and
//! wide enough to hold it; respects `NO_COLOR` and `LETHE_NO_ANIMATION`.

use std::io::{IsTerminal, Write};
use std::thread::sleep;
use std::time::Duration;

const FRAMES_RAW: &str = include_str!("avatar_frames.ansi");
const FRAME_MS: u64 = 60;

/// Restores the cursor on the way out (even on early return / panic).
struct CursorGuard;
impl Drop for CursorGuard {
    fn drop(&mut self) {
        let mut out = std::io::stdout();
        let _ = out.write_all(b"\x1b[?25h");
        let _ = out.flush();
    }
}

fn frames() -> Vec<&'static str> {
    FRAMES_RAW
        .split('\u{0c}')
        .map(|f| f.trim_matches('\n'))
        .filter(|f| !f.is_empty())
        .collect()
}

/// Display width of a line, ignoring ANSI escapes (chafa symbols are one cell).
fn display_width(line: &str) -> usize {
    let mut width = 0;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
            }
            while let Some(&n) = chars.peek() {
                chars.next();
                if n.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            width += 1;
        }
    }
    width
}

fn frame_cols(frame: &str) -> usize {
    frame.lines().map(display_width).max().unwrap_or(0)
}

fn animation_enabled() -> bool {
    std::env::var_os("LETHE_NO_ANIMATION").is_none()
        && std::env::var_os("LETHE_NO_AVATAR").is_none()
}

/// Print `below` and animate the avatar. On a tall+wide terminal the avatar
/// animates *above* the text with everything on screen at once; otherwise it
/// animates on its own and the text is printed beneath it afterward. Falls
/// back to plain text when there's no color TTY big enough for the avatar — so
/// callers can route *all* their output through this.
pub fn play_above(below: &[String]) {
    let frames = frames();
    let Some(&first) = frames.first() else {
        print_plain(below);
        return;
    };
    let rows = first.lines().count();
    if rows == 0 || !std::io::stdout().is_terminal() || std::env::var_os("NO_COLOR").is_some() {
        print_plain(below);
        return;
    }
    let avatar_cols = frame_cols(first);
    let (term_cols, term_rows) = match crossterm::terminal::size() {
        Ok((c, r)) => (c as usize, r as usize),
        Err(_) => {
            print_plain(below);
            return;
        }
    };
    // Need at least enough room to draw the avatar itself.
    if term_cols < avatar_cols || term_rows < rows + 1 {
        print_plain(below);
        return;
    }

    let s = below.len();
    let max_below = below.iter().map(|l| display_width(l)).max().unwrap_or(0);
    if term_cols >= max_below && term_rows >= rows + s + 1 {
        // Stacked: avatar animates above, text stays visible below it.
        animate(&frames, first, rows, below);
    } else {
        // Not enough room for both — animate the avatar, then print the text.
        animate(&frames, first, rows, &[]);
        print_plain(below);
    }
}

/// Render the (already size-checked) animation: paint frame 0 + `below`, then
/// redraw the avatar in place for one ~1.7s pass and settle on the calm frame.
fn animate(frames: &[&str], first: &str, rows: usize, below: &[String]) {
    let mut out = std::io::stdout().lock();
    let _guard = CursorGuard;
    let _ = out.write_all(b"\x1b[?25l");
    let s = below.len();

    let mut buf = String::new();
    push_frame(&mut buf, first);
    for line in below {
        buf.push_str(line);
        buf.push('\n');
    }
    let _ = out.write_all(buf.as_bytes());
    let _ = out.flush();

    if animation_enabled() {
        let height = rows + s; // distance from the bottom back to the avatar top
        let settle = [first];
        let sequence = frames.iter().skip(1).copied().chain(settle);
        for frame in sequence {
            let mut b = String::new();
            b.push_str(&format!("\x1b[{height}A\r")); // up to the avatar's top
            push_frame(&mut b, frame); // redraw avatar rows (text below untouched)
            if s > 0 {
                b.push_str(&format!("\x1b[{s}B")); // step back down past the text
            }
            let _ = out.write_all(b.as_bytes());
            let _ = out.flush();
            sleep(Duration::from_millis(FRAME_MS));
        }
    }
}

fn push_frame(buf: &mut String, frame: &str) {
    for line in frame.lines() {
        buf.push_str(line);
        // Reset color, then clear to end of line so a wider glyph from the
        // previous frame can't leave a ghost on the right edge.
        buf.push_str("\x1b[0m\x1b[K\n");
    }
}

fn print_plain(below: &[String]) {
    let mut out = std::io::stdout().lock();
    for line in below {
        let _ = writeln!(out, "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_parse_and_are_uniform() {
        let f = frames();
        assert!(f.len() > 5, "expected several frames, got {}", f.len());
        let rows = f[0].lines().count();
        assert!(rows > 5);
        for fr in &f {
            assert_eq!(fr.lines().count(), rows, "all frames must share a height");
        }
    }

    #[test]
    fn frames_have_no_baked_background() {
        // chafa --fg-only must not emit background-color SGR (48;…) codes, so
        // the terminal's own background shows through.
        assert!(
            !FRAMES_RAW.contains("48;2;"),
            "frames should not set a background color"
        );
    }

    #[test]
    fn display_width_ignores_ansi() {
        assert_eq!(display_width("\x1b[38;2;1;2;3mAB\x1b[0m"), 2);
        assert_eq!(display_width("abc"), 3);
    }

    #[test]
    fn frame_cols_reasonable() {
        assert!((30..=60).contains(&frame_cols(frames()[0])));
    }
}
