# Message Queue — Rate-Limit Recovery

Idempotent message queue for recovering from rate limits across any messaging
channel. When a send attempt fails due to a rate limit, this skill queues the
message, schedules a retry, and processes it when the limit clears.

## When to use

- A message send (iMessage, WhatsApp, Telegram, Signal, Discord, or any
  channel) returns a rate-limit error (429, `RATE_LIMITED`, `throttled`,
  `quota exceeded`, `retry-after`).
- A heartbeat or cron wake fires and pending messages need processing.
- You want to check the queue status.

## Queue state file

The queue lives at `message-queue-state.json` in the workspace root. Each
entry tracks:

- `id` — UUID, assigned on enqueue
- `dedupeKey` — `channel:chatId:timestamp:hash(messageText)`, prevents double-enqueue
- `status` — `pending | processing | sent | failed`
- `lockedBy` / `lockExpiresAt` — optimistic lock with 2-minute expiry
- `retryCount` — incremented on each failed attempt, max 5
- `rateLimitUntil` — ISO-8601 time when the rate limit clears

## State machine

```
pending → processing → sent
                     → failed (retry count >= 5)
                     → pending (retry count < 5, requeued)
```

No state can be skipped. A message must be locked (`processing`) before it can
be marked `sent`.

## Workflow

### Step 1: Enqueue on rate limit

When a send fails with a rate-limit error, run the `enqueue` command:

```bash
node scripts/mq.mjs enqueue \
  --channel whatsapp \
  --chat-id "+1234567890" \
  --sender "+1234567890" \
  --message "The original inbound message text" \
  --ts "2026-09-18T10:00:00Z" \
  --error "429 Too Many Requests, retry-after: 60"
```

This is idempotent. Enqueueing the same message twice (same channel, chatId,
timestamp, and content hash) is a no-op.

After enqueueing, schedule a cron wake event:

```
cron(action=add, job={
  name: "mq-recover-<short-id>",
  schedule: { kind: "at", at: "<rateLimitUntil>" },
  payload: { kind: "systemEvent",
    text: "Rate limit cleared. Process the message queue: read message-queue-state.json, find pending entries, respond to each, update status." },
  deleteAfterRun: true,
  sessionTarget: "main"
})
```

### Step 2: Process on wake or heartbeat

When a cron wake or heartbeat fires, run:

```bash
node scripts/mq.mjs process
```

This acquires optimistic locks on all ready entries (past their
`rateLimitUntil`), preventing double-sends from concurrent processors. For
each locked entry:

1. Read the chat history for context.
2. Draft and send the response via the appropriate channel.
3. Run `node scripts/mq.mjs ack --id <entryId> --lock <lockId> --status sent`
   on success.
4. Run `node scripts/mq.mjs ack --id <entryId> --lock <lockId> --status retry --error "..."` on rate limit again.

### Step 3: Check status

```bash
node scripts/mq.mjs status
```

Returns counts by status, next retry time, and oldest pending entry.

## Rate limit patterns recognized

- `429 Too Many Requests`
- `rate_limit`, `Rate limit exceeded`
- `RATE_LIMITED`, `throttled`
- `quota exceeded`
- `retry-after: <seconds>`
- `Resets at <ISO-8601 timestamp>`

When no retry-after is parseable, defaults to 60 seconds.

## Heartbeat integration

Add to `HEARTBEAT.md`:

```markdown
## Pending message queue
- Run `node scripts/mq.mjs status` to check for pending entries.
- If any entries have status "pending" and rateLimitUntil has passed, run
  `node scripts/mq.mjs process` and handle each locked entry.
```

## Idempotency guarantees

1. **Deduplication** prevents double-enqueue from retried tool calls.
2. **Optimistic locking** prevents double-sends from concurrent processors.
3. **Lock expiry** (2 minutes) recovers from crashed processors without
   manual intervention.
4. **Atomic file writes** (temp + rename) prevent corrupted reads.
5. **Max 5 retries** prevents infinite retry loops.
