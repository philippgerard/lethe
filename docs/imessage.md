# iMessage through Linq

Lethe can receive text from an allowlisted direct conversation and reply through
Linq's V3 API. The transport runs in the existing HTTP API process and uses the
same Agent and Brainstem as the other transports. It is disabled by default.
It does not connect to your personal Apple ID or read your local Messages app.

Linq documents a permanent free Hobby shared line with up to 20 contacts through
[CLI signup](https://linqapp.com/cli): the owner runs `linq signup` and completes
email verification. The dashboard's business sandbox trial is a separate offer;
a browser-based permanent Hobby signup path has not been verified.
Shared lines are inbound-first: add your contact in Linq, then text the assigned
line before expecting a reply. The Linq contact list and Lethe's sender allowlist
are separate requirements. See [Linq's CLI guide](https://github.com/linq-team/linq-cli#quick-start)
and [current pricing](https://linqapp.com/s/pricing). Lethe calls the API directly;
the Linq CLI is an optional account-management tool, not a runtime dependency.

## Configuration

Provision the Linq account, contacts, API token, and webhook subscription outside
the conversation. Put tokens and signing secrets in the runtime's protected
environment or secret manager; never paste them into Messages or a model prompt.
This guide does not perform signup, install a CLI, start a service, or send a
test message.

These settings are read at API process startup. Restart the process after
changing Linq credentials, the sender allowlist, or poll enablement; the existing
Telegram control-plane hot configuration does not manage Linq.

For the companion `lethe-stack` deployment, the Compose declarations pass these
settings from Dokploy's protected environment. Its pinned release must first be
updated to a release containing this integration. The existing Tailscale proxy
is private; a separate public HTTPS route restricted to the webhook path is
required before Linq can reach it.

For the prepared `lethe-stack` hostname, register this final callback URL and
explicitly pin its payload version:

```text
https://lethe-imessage.philippgerard.de/webhooks/linq?version=2026-02-03
```

The disabled routing draft is `examples/linq-webhook.compose.yml` in the
[companion stack repository](https://github.com/philippgerard/lethe-stack). Its
host, exact path, and POST method restriction accepts the version query without
exposing other API routes. Preparing the draft does not activate it.

| Setting | Purpose | Default |
| --- | --- | --- |
| `LINQ_ENABLED` | Enable the Linq transport in API mode. | `false` |
| `LINQ_API_TOKEN` | Linq bearer token used for outgoing messages. | unset |
| `LINQ_WEBHOOK_SECRET` | Signing secret for the Linq webhook subscription. | unset |
| `LINQ_ALLOWED_SENDERS` | Comma-separated exact sender handles permitted to use Lethe. | unset |
| `LINQ_NATIVE_POLLS` | Attempt optional iMessage polls alongside text controls. | `false` |
| `LETHE_API_TOKEN` | Lethe API authentication; distinct from the Linq secrets. | required in API mode |

An enabled transport requires all three of `LINQ_API_TOKEN`,
`LINQ_WEBHOOK_SECRET`, and a nonempty `LINQ_ALLOWED_SENDERS`. For a phone sender,
use the complete E.164 handle that Linq supplies, such as the synthetic example
`+15555550123`. Matches are exact; Lethe does not enroll the first person who
texts the line. Group conversations are rejected even when a participant is
allowlisted. Incoming iMessage, RCS, and SMS text are accepted; attachments and
voice transcription are not implemented by this transport.

Expose only `POST /webhooks/linq` through an HTTPS reverse proxy or a provider
webhook forwarder. Keep the rest of the API private or behind its normal Lethe
authentication. Preserve the raw request body and the `webhook-id`,
`webhook-timestamp`, and `webhook-signature` headers. Configure the subscription
for API `v3`, webhook version **`2026-02-03`**, and these events:

- `message.received` for inbound text.
- `message.sent`, `message.delivered`, `message.read`, and `message.failed` for
  delivery observations.
- `poll.vote.added` if native polls are explicitly enabled. Removed votes do not
  authorize an action.

Lethe verifies the Standard Webhooks HMAC signature before parsing input, rejects
timestamps outside a five-minute window, and deduplicates inbound event IDs in
durable storage. See [Linq's webhook guide](https://docs.linqapp.com/channel/imessage/guides/webhooks/).
The webhook has a 1 MiB body limit; accepted text has a 32 KiB limit.

Outgoing Markdown is rendered as readable text with Linq's native bold and italic
decorations, including correct UTF-16 ranges for emoji and long-message chunks.
Messages explicitly sent with `parse_mode: "html"` use an HTML renderer, including
scheduled notifications routed from Telegram tools. Supported emphasis becomes
native formatting, entities are decoded, and links retain their destinations.
Literal HTML in ordinary Markdown and code remains literal.
Links retain their URLs and code retains its literal content. Struck-out text
also carries an explicit label so its meaning survives SMS/RCS, which ignore
decorations. Rendered chunks are persisted before sending, so delivery retries
reuse identical payloads. Previously queued messages retain their original
payloads. Whitespace-only pieces are rebalanced where possible; standalone
excess whitespace is omitted because Linq rejects empty-looking text parts.
See [Linq's sending guide](https://docs.linqapp.com/channel/imessage/guides/messaging/sending-messages/).

`GET /imessage/status` requires `Authorization: Bearer <LETHE_API_TOKEN>` or
`x-lethe-token`. It reports enablement, queue counts, interrupted turns, and
provider/device delivery observations without revealing secrets. An unsigned
webhook returns 401; an incompatible payload or version returns 400; a disabled
transport returns 404. A newly persisted input returns 202 with
`accepted: true`. Duplicates and ignored events return 200 with
`accepted: false`. These responses acknowledge ingestion, not task completion.

## Decisions in Messages

Lethe can park one exact tool invocation with `request_action_approval`, including
the immutable arguments and a human-readable summary. Review the merchant or
recipient, item, options, quantity, price, and commitment terms before approving.
The default expiry is ten minutes; a request may specify 30–1800 seconds.
Decisions are bound to the provider, direct chat, and sender that received the
request. A bare “yes”, a forwarded request, or a choice in another conversation
does not authorize that invocation.

| Text command | Effect |
| --- | --- |
| `/approve <request-id>` | Approve the displayed invocation and start a continuation instructed to recheck its terms before execution. |
| `/reject <request-id>` | Reject a pending invocation. |
| `/approvals` | Show pending and approved, unconsumed requests in this conversation. |
| `/resume <request-id>` | Continue an approved, unconsumed, unexpired request after checking the current state. |
| `/cancel` | Stop this conversation's current iMessage turn and clear its queued input. Independently running workers continue. |
| `/notifications` or `/notifications status` | Show Telegram and iMessage notification settings. |
| `/notifications on` | Link this conversation and enable iMessage notifications. |
| `/notifications off` | Disable iMessage notifications; automatically enable Telegram if it was the last channel on. |

`execute_approved_action(request_id)` consumes an approved payload before
dispatching its saved arguments. It cannot be reused, including when a tool
fails, becomes unavailable, times out, or a process stops during dispatch.
Inspect the external result before requesting a new approval. `/resume` does not
replay a consumed request. Cancellation cannot undo an external action that has
already happened.

This is a runtime check for calls routed through the approval tools. It is not a
universal interception layer for every available tool or browser click. The
bundled browser workflow requires consequential actions to use this path on
iMessage; worker scope, state inspection, and the stopping boundary still matter.
Other transports use their existing explicit authorization flow.

`chat_send_choices` asks a preference question with two to four options; choosing
one is not approval of a consequential action. Text remains the default for
choices and approvals. Native polls are optional, iMessage-only, and fall back
to the text instructions if sending the poll fails. Hobby poll entitlement has
not been verified; leave `LINQ_NATIVE_POLLS=false` until the account's support is
confirmed. See [Linq's poll documentation](https://docs.linqapp.com/channel/imessage/guides/messaging/polls/).

Custom branded controls inside Messages require a separate shipping Messages app
extension installed by the recipient; Linq lists iMessage Apps on its Pro tier.
Linq also documents its own hosted Experiences. This integration ships neither
custom app controls nor Experience cards. See [iMessage Apps](https://docs.linqapp.com/channel/imessage/guides/messaging/imessage-apps/)
and [pricing](https://linqapp.com/s/pricing).

## Credentials and browser handoff

Messages carries task text and decisions, not password or OTP entry. Continue to
use the Alien vault-sealed browser tools. A missing credential requires the
existing secure-input flow in a connected, authenticated Lethe client that
supports its credential card. Hosted deployments need the existing
`LETHE_SECURE_PROMPT=hosted` integration; enabling Linq does not enable it.
Human-only challenges require that client's live
browser viewport. There is no secure credential card or browser takeover UI in
the iMessage transport itself. Keep the session open, complete the handoff in
Lethe, then ask it to inspect the current page before continuing.

The bundled [browser workflow](../config/skills/browser-workflows.md) describes
bounded workers, ordinary form batching, verification, exact approvals, and
human takeover using the existing Alien surface. Local memory initialization
seeds it as `workspace/skills/browser-workflows.md` without overwriting an
existing file. Workflows are instructions, not additional browser primitives or
a guarantee of task success. API, Telegram, and iMessage work can share the same
browser; concurrent use is not serialized across transports. Avoid simultaneous
browser tasks.

The [browser evaluation suite](../evals/browser/README.md) includes five synthetic
fixture cases and a separately selected read-only public-site case. Its default
command only previews cases. Executing a case requires an explicit `--run`, a
selected test instance and case, and API authentication. Passing offline runner
tests does not establish live browser reliability; visible outcomes and stopping
boundaries need manual grading. No account signup, purchase, credential entry,
or live evaluation is part of ordinary repository validation.

## Background updates and recovery

Telegram notifications start on; iMessage starts off until linked. Existing
iMessage subscriptions are preserved during upgrade. In either private chat,
`/notifications` shows both settings and `/notifications on` or `/notifications off`
changes that channel. Both may be on. Turning off the last enabled channel turns
on the other and explains the switch. If the other channel is not configured or
iMessage has never been linked, the current channel stays on with an explanation.
These commands run locally without a model call. Replies to your own messages,
including command confirmations, always return to the originating chat.

The iMessage destination and preferences persist across restarts. Turning it off
retains the linked destination for automatic reactivation, but invalidates unsent
notifications; turning it back on does not replay those old notices. Linking from
another allowed direct conversation replaces the destination. Delivery rechecks
the sender allowlist and current preference. An outgoing request already in
progress cannot be recalled. If iMessage becomes unavailable or its sender is
removed, Telegram is selected as the remaining usable channel.

Reviewed Brainstem emissions, scheduled `/wake` messages and failure notices,
and generated actor updates follow these preferences. A Telegram send failure
does not suppress durable iMessage queueing when iMessage is enabled. Explicit
wakes to other Telegram recipients are excluded from owner routing.
Quiet scheduled turns enqueue nothing. Attachments still require Telegram;
when Telegram is off, the tool returns an error asking for a text summary.
When both channels are on, an attachment sent to Telegram is described in the
iMessage notification with its caption; the file itself is not uploaded there.
Raw actor messages and every worker completion are not forwarded automatically. Existing `HEARTBEAT_ENABLED`,
`HEARTBEAT_INTERVAL`, `PROACTIVE_MAX_PER_DAY`, and
`PROACTIVE_COOLDOWN_MINUTES` govern proactive evaluation and rate limits.

Transport state is stored under `<LETHE_HOME>/data/transports/` in `linq.sqlite`
and `actions.sqlite`. Treat these databases as private: they contain conversation
text and proposed action arguments. An interrupted model turn is marked
interrupted on restart and a recovery notice is queued; it is never automatically
replayed. A model turn is bounded to fifteen minutes. Inspect external state
before repeating work after a cancellation, timeout, or recovery notice.

Outgoing messages use a durable outbox with stable provider idempotency keys,
bounded retries, and a one-hour queue deadline. Queued messages to a sender
removed from the allowlist are skipped. Early poll votes wait durably for their
option mapping for up to one hour; transient poll errors retry with the same
key. Provider acceptance is distinct
from device delivery or reading. Delivery webhooks improve the status evidence,
but missing receipts are not proof of failure. Delivery retries do not rerun a
model turn or an approved action. No live Linq delivery or Hobby feature coverage
is implied by offline tests.
