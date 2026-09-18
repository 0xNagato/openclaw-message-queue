# @openclaw/plugin-message-queue

Idempotent message queue with rate-limit recovery for OpenClaw. When the agent
hits a rate limit sending a response on any channel (iMessage, WhatsApp,
Telegram, Signal, Discord), this plugin queues the message and retries
automatically when the limit clears.

## How it works

1. Agent attempts to send a response, gets a rate-limit error.
2. Agent calls `mq_enqueue` with the inbound message details and error.
3. Plugin deduplicates, parses the retry-after window, persists to disk.
4. Agent schedules a `cron` wake event for when the limit clears.
5. On wake, agent calls `mq_process` to lock and process ready messages.
6. After each send, agent calls `mq_ack` to mark sent or requeue.

Every operation is idempotent. Enqueueing the same message twice is a no-op.
Processing uses optimistic locking with expiry so concurrent heartbeats and
cron wakes never double-send. State transitions follow a strict machine:
`pending → processing → sent|failed`, with `processing → pending` for retries.

## Tools

| Tool | Purpose |
|------|---------|
| `mq_enqueue` | Queue a message that failed due to rate limit |
| `mq_process` | Lock and process ready messages (idempotent) |
| `mq_ack` | Mark a processed message as sent or retry |
| `mq_status` | Current queue counts, next retry time |

## Install

```bash
openclaw plugins install clawhub:@openclaw/plugin-message-queue
```

Or for local development:

```bash
cd openclaw-message-queue
pnpm install
pnpm run plugin:build
openclaw plugins install ./
```

## Config

```json
{
  "plugins": {
    "entries": {
      "message-queue": {
        "config": {
          "queuePath": "/path/to/message-queue-state.json",
          "maxRetries": 5,
          "defaultRetryDelayMs": 60000
        }
      }
    }
  }
}
```

## Design

**Idempotency.** Deduplication by `channel:chatId:timestamp:contentHash`.
Optimistic locking prevents double-sends across concurrent processors.

**State machine.** `pending → processing → sent | failed`. No skipped states.
Lock expiry (2 minutes) recovers from crashed processors.

**Atomic persistence.** Write to temp file, rename. No partial reads.

**Rate limit parsing.** Recognizes `429`, `retry-after`, `RATE_LIMITED`,
`throttled`, `quota exceeded`, and ISO timestamp patterns.
