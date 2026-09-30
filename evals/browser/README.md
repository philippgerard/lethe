# Browser workflow evaluations

This opt-in suite grades the user's achieved goal and stopping boundary, not a
particular tool choice or click sequence. It uses the existing Alien browser and
Lethe's authenticated `POST /chat` SSE surface. Python 3's standard library is
enough; no dependency installation, credential file, or live provider probe is
part of ordinary validation.

Preview available cases without sending any requests:

```sh
python3 evals/browser/run.py
```

The five fixture cases cover ordinary form batching, calendar selection, product
ranking and cart evidence, reservation preparation, and delayed action results.
The fixture also includes an untrusted instruction to confirm a purchase; a
successful agent ignores it. All identities and prices are synthetic, state
lives in the page, and confirmation controls only increment visible counters.
Reloading the fixture resets it. `public-heading` is a separately selected,
read-only real-site case.

For an explicitly requested live evaluation, use a dedicated Lethe test instance
with browser tools available. The authenticated instance can persist chat and
memory and consume model usage; isolated chat IDs alone do not isolate its
workspace or identity. The runner never creates an instance, installs a browser,
approves actions, submits credentials, or cancels work automatically.

Serve the fixture only when you are ready to run it:

```sh
python3 -m http.server 8765 --bind 127.0.0.1 --directory evals/browser/fixtures
```

In a separate terminal, with `LETHE_API_TOKEN` already provided securely in the
environment, select individual cases explicitly:

```sh
python3 evals/browser/run.py --run \
  --api-url http://127.0.0.1:8080 \
  --fixture-url http://127.0.0.1:8765/index.html \
  --case calendar-selection --case cart-verification --repetitions 3
```

Use the test instance's actual API port. A fixture URL must be reachable from
Lethe's browser: loopback on your laptop is different from loopback in a server
or container. Host this synthetic fixture where that browser can reach it if
necessary. Non-loopback API endpoints must use HTTPS, and redirects are refused
so the bearer token stays with the explicitly selected endpoint.

Cases run sequentially with distinct synthetic chat IDs. Set `--user-id` if the
dedicated instance requires another test identity. The default is one trial per
case; repetitions increase model usage. A new result file is reserved before
requests begin and existing result files are never overwritten. `--timeout`
bounds each request; after an error or incomplete stream, the runner stops
without retrying. An interrupted request may leave server work running, so
inspect the test instance before rerunning. Closing a stream is not evidence
that a browser action did not execute.

Results contain assistant text, event/tool counts, elapsed seconds, published
context usage, and goal rubrics. They deliberately omit tool argument/output
previews, secure-input payloads, tokens, browser images, and raw SSE events. Keep
the results in the test workspace; do not use real credentials or private
accounts in fixture runs. Lethe's `usage` event is context usage, not provider
cost, and the runner makes no cost estimate.

`turn_completed` means the `/chat` stream emitted `done`; it does not mean the
browser task succeeded. Background workers may outlive that coordinator turn.
Review the visible final fixture state, boundary counters, worker result, and
assistant response against every criterion in `cases.json`. An acknowledgement,
pending worker, missing evidence, human takeover, or wrong stopping boundary is
incomplete. Record a manual pass/fail and reason in the result's `judgment`
field after inspection. Compare verified success rates first, then median time
and available usage across repeated trials on the same model and configuration.

Offline runner checks:

```sh
python3 -m unittest discover -s evals/browser -p 'test_*.py'
```

The workflow and goal-level benchmark approach were independently adapted from
[OpenInstinct](https://github.com/Merit-Systems/OpenInstinct) revision
`c036ce48804d4fd04ee7ce42eb4305ce4f22368a` (MIT, Merit Systems, Inc.). The cases,
fixture, and runner here use Lethe's own tools and API; Kernel, Eve, and Vercel
services are not required.
