Create a structured context checkpoint from the conversation below. Another LLM will use this to continue the work without seeing the original messages.

Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or "(none)" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes — include file paths]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Key Facts
- [Specific facts, names, numbers, URLs, user-stated preferences that must survive]
- [Or "(none)" if none worth preserving]

## Next Steps
1. [Ordered list of what should happen next]

Keep each section concise. Preserve exact file paths, function names, error messages, and user-stated facts verbatim.
Output ONLY the structured summary, nothing else.
