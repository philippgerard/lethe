"""Claude (Anthropic) context assembler."""

from typing import Dict, Optional

from lethe.context import ContextAssembler, SystemComponents, register
from lethe.prompts import load_prompt_template


@register
class ClaudeAssembler(ContextAssembler):

    model_patterns = ["claude", "anthropic"]

    def get_prompt_insertions(self, components: SystemComponents) -> list[str]:
        persona = load_prompt_template("claude_persona")
        return [persona] if persona else []

    def get_comm_rules_filename(self) -> Optional[str]:
        return "communication-anthropic.md"

    def get_identity_cache_control(self) -> Dict[str, str]:
        return {"type": "ephemeral", "ttl": "1h"}

    def should_embed_tool_reference(self) -> bool:
        return False
