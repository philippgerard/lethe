"""Emotional salience tracking for memory recall bias and context hints."""

from __future__ import annotations

import json
import logging
import os
import re
from collections import deque
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

from lethe.paths import workspace_dir
from lethe.prompts import load_prompt_template

logger = logging.getLogger(__name__)

SALIENCE_TAGS_FILE = str(workspace_dir() / "emotional_tags.md")

HIGH_AROUSAL_THRESHOLD = 0.75
TAG_LOG_MAX_LINES = 300
TAG_LOG_KEEP_LINES = 140
FLASHBACK_LOOKBACK = 12

SALIENCE_USER_PROMPT = load_prompt_template(
    "amygdala_seed_user",
    fallback=(
        "Classify the most recent user signals.\n"
        "- valence in [-1,1], arousal/confidence in [0,1]\n"
        "- capture sarcasm and mixed affect\n"
        "- output max 8 items as JSON array only\n\n"
        "Previous state summary:\n{previous_state}\n\n"
        "Recent signals:\n{recent_signals}\n"
    ),
)


class SalienceTracker:
    """Tracks emotional salience independently from associative recall."""

    def __init__(
        self,
        classifier: Optional[Callable[[str], Awaitable[str]]] = None,
        tags_file: str = SALIENCE_TAGS_FILE,
    ):
        self.classifier = classifier
        self.tags_file = tags_file
        self._stats = {
            "tags_total": 0,
            "tags_high_arousal": 0,
            "last_tagged_at": "",
            "last_error": "",
            "tags_pruned_total": 0,
        }
        self._active_patterns: deque[str] = deque(maxlen=FLASHBACK_LOOKBACK)

    @property
    def enabled(self) -> bool:
        return self.classifier is not None

    @property
    def active_patterns(self) -> list[str]:
        return list(self._active_patterns)

    async def tag(self, message) -> None:
        """Classify emotional salience for a user message and append tags."""
        if not self.classifier:
            return

        try:
            text = self._extract_text(message)
            if not text or len(text) < 5:
                return

            prompt = SALIENCE_USER_PROMPT.format(
                previous_state=self.read_tags_tail(max_lines=30) or "(none)",
                recent_signals=text[:2000],
            )
            raw = await self.classifier(prompt)
            if not raw:
                return

            tags = self.parse_tags(raw)
            if not tags:
                return

            self.append_tags(tags)
            self.update_active_patterns(tags)
            self._stats["tags_total"] += len(tags)
            self._stats["tags_high_arousal"] += sum(1 for t in tags if t.get("high_arousal"))
            self._stats["last_tagged_at"] = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            logger.warning("Salience tagging failed: %s", e)
            self._stats["last_error"] = str(e)

    @staticmethod
    def _extract_text(message) -> str:
        if isinstance(message, list):
            text_parts = [
                p.get("text", "")
                for p in message
                if isinstance(p, dict) and p.get("type") == "text"
            ]
            return " ".join(text_parts).strip()
        return str(message).strip()

    @staticmethod
    def parse_tags(raw: str) -> list[dict]:
        """Parse LLM output into normalized salience tag dicts."""
        candidate = raw.strip()
        if candidate.startswith("```"):
            candidate = re.sub(r"^```(?:json)?\s*", "", candidate, flags=re.IGNORECASE)
            candidate = re.sub(r"\s*```$", "", candidate)

        try:
            data = json.loads(candidate)
        except Exception:
            start = candidate.find("[")
            end = candidate.rfind("]")
            if start == -1 or end == -1 or end <= start:
                return []
            try:
                data = json.loads(candidate[start : end + 1])
            except Exception:
                return []

        if not isinstance(data, list):
            return []

        normalized = []
        for item in data[:8]:
            if not isinstance(item, dict):
                continue
            signal = str(item.get("signal", "")).strip()[:180]
            if not signal:
                continue
            try:
                valence = max(-1.0, min(1.0, float(item.get("valence", 0.0))))
            except Exception:
                valence = 0.0
            try:
                arousal = max(0.0, min(1.0, float(item.get("arousal", 0.0))))
            except Exception:
                arousal = 0.0
            try:
                confidence = max(0.0, min(1.0, float(item.get("confidence", 0.5))))
            except Exception:
                confidence = 0.5
            raw_tags = item.get("tags", [])
            if not isinstance(raw_tags, list):
                raw_tags = [str(raw_tags)]
            tags = [str(t).strip()[:32] for t in raw_tags if str(t).strip()]
            normalized.append(
                {
                    "signal": signal,
                    "valence": round(valence, 2),
                    "arousal": round(arousal, 2),
                    "confidence": round(confidence, 2),
                    "tags": tags or ["neutral"],
                    "high_arousal": arousal >= HIGH_AROUSAL_THRESHOLD,
                }
            )
        return normalized

    def append_tags(self, tags: list[dict]) -> None:
        """Append salience tags to the rolling tag log and compact if needed."""
        if not tags:
            return

        now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
        lines = [f"## {now}"]
        for item in tags:
            high_arousal_marker = " ⚡" if item.get("high_arousal") else ""
            lines.append(
                f"- [{item['valence']:+.2f}v {item['arousal']:.2f}a] "
                f"{', '.join(item.get('tags', []))}: {item['signal'][:120]}{high_arousal_marker}"
            )
        lines.append("")

        try:
            with open(self.tags_file, "a") as f:
                f.write("\n".join(lines) + "\n")
        except Exception as e:
            logger.warning("Failed to write salience tags: %s", e)
            return

        self.compact_tag_log()

    def compact_tag_log(self) -> None:
        """Keep the emotional tag log bounded while preserving recent state."""
        try:
            if not os.path.exists(self.tags_file):
                return
            with open(self.tags_file, "r") as f:
                lines = f.read().splitlines()
            if len(lines) <= TAG_LOG_MAX_LINES:
                return

            keep = lines[-TAG_LOG_KEEP_LINES:]
            pruned = len(lines) - len(keep)
            now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
            header = [
                f"# Emotional tags (compacted at {now})",
                f"- pruned_lines: {pruned}",
                "- note: keeping only recent rolling window",
                "",
            ]
            with open(self.tags_file, "w") as f:
                f.write("\n".join(header + keep).strip() + "\n")
            self._stats["tags_pruned_total"] = (
                int(self._stats.get("tags_pruned_total", 0)) + pruned
            )
        except Exception as e:
            logger.warning("Failed to compact tag log: %s", e)

    def update_active_patterns(self, tags: list[dict]) -> None:
        """Track high-arousal tag patterns for recall bias."""
        for item in tags:
            if not item.get("high_arousal"):
                continue
            tag_list = item.get("tags", [])
            if isinstance(tag_list, list) and tag_list:
                self._active_patterns.append(str(tag_list[0]))

    def get_emotional_boost(self) -> str:
        """Return recent high-arousal keywords to bias memory search."""
        if not self._active_patterns:
            return ""

        seen = set()
        keywords = []
        for pattern in reversed(self._active_patterns):
            if pattern not in seen:
                seen.add(pattern)
                keywords.append(pattern)
            if len(keywords) >= 3:
                break
        return " ".join(keywords)

    def get_emotional_state(self) -> Optional[str]:
        """Build a one-line emotional state summary from recent salience tags."""
        tail = self.read_tags_tail(max_lines=20)
        if not tail or len(tail.strip()) < 10:
            return None

        recent_tags = []
        high_arousal_signals = []
        for line in tail.splitlines():
            line = line.strip()
            if not line.startswith("- ["):
                continue
            try:
                bracket_end = line.index("]", 3)
                scores = line[3:bracket_end]
                rest = line[bracket_end + 2 :]
                parts = scores.split()
                arousal = float(parts[1].rstrip("a"))
                valence = float(parts[0].rstrip("v"))
                colon_idx = rest.find(":")
                if colon_idx > 0:
                    tags_str = rest[:colon_idx].strip()
                    signal = rest[colon_idx + 1 :].strip().rstrip("⚡").strip()
                else:
                    tags_str = rest.strip()
                    signal = ""
                tags = [t.strip() for t in tags_str.split(",") if t.strip()]
                recent_tags.extend(tags)
                if arousal >= HIGH_AROUSAL_THRESHOLD:
                    high_arousal_signals.append((valence, tags, signal[:60]))
            except (ValueError, IndexError):
                continue

        if not recent_tags:
            return None

        parts = []
        if high_arousal_signals:
            valence, tags, signal = high_arousal_signals[-1]
            if valence < -0.3:
                mood = "distressed"
            elif valence < 0:
                mood = "tense"
            elif valence > 0.3:
                mood = "excited"
            else:
                mood = "aroused"
            parts.append(f"{mood} (high arousal)")
            if signal:
                parts.append(f"about: {signal}")

        if self._active_patterns:
            seen = set()
            patterns = []
            for pattern in reversed(self._active_patterns):
                if pattern not in seen:
                    seen.add(pattern)
                    patterns.append(pattern)
                if len(patterns) >= 3:
                    break
            if patterns:
                parts.append(f"recurring: {', '.join(patterns)}")

        if not parts:
            return None
        return f"[Emotional state: {'; '.join(parts)}]"

    def read_tags_tail(self, max_lines: int = 30) -> str:
        """Read the last N lines of this tracker's tag file."""
        return self._read_tags_tail(self.tags_file, max_lines=max_lines)

    @staticmethod
    def _read_tags_tail(tags_file: str = SALIENCE_TAGS_FILE, max_lines: int = 30) -> str:
        try:
            if not os.path.exists(tags_file):
                return ""
            with open(tags_file, "r") as f:
                lines = f.read().splitlines()
            if not lines:
                return ""
            tail = lines[-max_lines:] if len(lines) > max_lines else lines
            return "\n".join(tail)
        except Exception:
            return ""

    def get_stats(self) -> dict:
        stats = dict(self._stats)
        stats["active_patterns"] = list(self._active_patterns)
        return stats
