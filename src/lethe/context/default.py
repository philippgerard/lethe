"""Default context assembler — baseline behavior for unrecognized models."""

from lethe.context import ContextAssembler, register


@register
class DefaultAssembler(ContextAssembler):
    """Fallback assembler registered for unknown models."""

    model_patterns = []  # Never matched — used as explicit fallback
