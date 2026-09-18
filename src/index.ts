import { Type } from "typebox";
import { defineToolPlugin } from "openclaw/plugin-sdk/tool-plugin";
import { loadState, saveState, enqueue, acquireLock, markSent, markFailed, releaseExpiredLocks, pruneCompleted, pendingReadyEntries, emptyState } from "./state.js";
import { isRateLimitError, parseRetryAfter } from "./rate-limit.js";
import { randomUUID } from "node:crypto";
import { join } from "node:path";

export default defineToolPlugin({
  id: "message-queue",
  name: "Message Queue",
  description:
    "Idempotent message queue with rate-limit recovery. Enqueue messages that fail due to rate limits, process them when limits clear.",
  configSchema: Type.Object({
    queuePath: Type.Optional(
      Type.String({ description: "Absolute path to queue state file." }),
    ),
    maxRetries: Type.Optional(
      Type.Number({
        description: "Max retries per message.",
        default: 5,
      }),
    ),
    defaultRetryDelayMs: Type.Optional(
      Type.Number({
        description: "Default retry delay in ms when retry-after is unparseable.",
        default: 60_000,
      }),
    ),
  }),
  tools: (tool) => [
    tool({
      name: "mq_enqueue",
      label: "Enqueue Message",
      description: `Idempotently enqueue a message that failed to send due to a rate limit.
Deduplicates by channel + chatId + timestamp + content hash.
Returns the entry ID and whether it already existed.
Call this whenever a send attempt returns a rate-limit error.`,
      parameters: Type.Object({
        channel: Type.String({
          description: "Channel id: imessage, whatsapp, telegram, signal, discord, etc.",
        }),
        chatId: Type.String({
          description: "Chat identifier (chat_id for imsg, target for message tool).",
        }),
        sender: Type.String({
          description: "Sender handle, phone number, or display name.",
        }),
        messageText: Type.String({
          description: "The inbound message text that needs a response.",
        }),
        ts: Type.String({
          description: "ISO-8601 timestamp of the original inbound message.",
        }),
        rateLimitError: Type.String({
          description: "The raw error message from the failed send attempt.",
        }),
      }),
      async execute(params, config, context) {
        context.signal?.throwIfAborted();
        const queuePath = resolveQueuePath(config);
        const defaultDelay = config.defaultRetryDelayMs ?? 60_000;

        if (!isRateLimitError(params.rateLimitError)) {
          return {
            enqueued: false,
            reason: "Error does not appear to be a rate limit. Not queuing.",
            error: params.rateLimitError,
          };
        }

        const retryAt = parseRetryAfter(params.rateLimitError, defaultDelay);
        let state = await loadState(queuePath);
        state = releaseExpiredLocks(state);

        const result = enqueue(state, {
          ts: params.ts,
          channel: params.channel,
          chatId: params.chatId,
          sender: params.sender,
          messageText: params.messageText,
          rateLimitError: params.rateLimitError,
          rateLimitUntil: retryAt,
          retryCount: 0,
        });

        await saveState(queuePath, result.state);

        return {
          enqueued: !result.alreadyExists,
          entryId: result.entryId,
          alreadyExists: result.alreadyExists,
          rateLimitUntil: retryAt,
          instruction: result.alreadyExists
            ? "This message is already queued. No action needed."
            : `Queued. Schedule a cron wake event at ${retryAt} with: cron(action=add, job={ name: "mq-recover-${result.entryId.slice(0, 8)}", schedule: { kind: "at", at: "${retryAt}" }, payload: { kind: "systemEvent", text: "Rate limit cleared. Run mq_process to send queued messages." }, deleteAfterRun: true, sessionTarget: "main" })`,
        };
      },
    }),

    tool({
      name: "mq_process",
      label: "Process Queue",
      description: `Process all pending messages whose rate limits have cleared.
Idempotent: safe to call repeatedly. Uses optimistic locking to prevent
double-sends. Returns a summary of what was processed.
Call this from heartbeat checks and cron wake events.`,
      parameters: Type.Object({
        dryRun: Type.Optional(
          Type.Boolean({
            description: "If true, report what would be processed without sending.",
            default: false,
          }),
        ),
      }),
      async execute(params, config, context) {
        context.signal?.throwIfAborted();
        const queuePath = resolveQueuePath(config);
        let state = await loadState(queuePath);
        state = releaseExpiredLocks(state);

        const ready = pendingReadyEntries(state);

        if (ready.length === 0) {
          state = pruneCompleted(state);
          await saveState(queuePath, state);
          return {
            processed: 0,
            pending: state.entries.filter((e) => e.status === "pending").length,
            message: "No messages ready to process.",
          };
        }

        if (params.dryRun) {
          return {
            dryRun: true,
            ready: ready.map((e) => ({
              id: e.id,
              channel: e.channel,
              chatId: e.chatId,
              sender: e.sender,
              messagePreview: e.messageText.slice(0, 100),
              retryCount: e.retryCount,
            })),
          };
        }

        const lockId = randomUUID();
        const results: Array<{
          id: string;
          channel: string;
          chatId: string;
          sender: string;
          messagePreview: string;
          locked: boolean;
        }> = [];

        for (const entry of ready) {
          const lockResult = acquireLock(state, entry.id, lockId);
          state = lockResult.state;
          results.push({
            id: entry.id,
            channel: entry.channel,
            chatId: entry.chatId,
            sender: entry.sender,
            messagePreview: entry.messageText.slice(0, 100),
            locked: lockResult.acquired,
          });
        }

        state = pruneCompleted(state);
        await saveState(queuePath, state);

        const locked = results.filter((r) => r.locked);

        return {
          processed: locked.length,
          skipped: results.length - locked.length,
          total_pending: state.entries.filter((e) => e.status === "pending").length,
          entries: locked,
          instruction: locked.length > 0
            ? `${locked.length} message(s) locked for processing. For each entry, read the chat history for context, draft a response, and send it via the appropriate channel. After each successful send, call mq_ack with the entry id and lockId "${lockId}". If a send fails with a rate limit, call mq_ack with status "retry" and the error message.`
            : "No messages could be locked. They may be processing elsewhere.",
          lockId,
        };
      },
    }),

    tool({
      name: "mq_ack",
      label: "Acknowledge Message",
      description: `Mark a queued message as sent or failed after attempting to send it.
Requires the lockId from mq_process. Idempotent: acknowledging an
already-acked message is a no-op.`,
      parameters: Type.Object({
        entryId: Type.String({ description: "Queue entry ID from mq_process." }),
        lockId: Type.String({ description: "Lock ID from mq_process." }),
        status: Type.Union([Type.Literal("sent"), Type.Literal("retry")], {
          description: "'sent' if the message was delivered, 'retry' if rate-limited again.",
        }),
        error: Type.Optional(
          Type.String({
            description: "Error message if status is 'retry'.",
          }),
        ),
      }),
      async execute(params, config, context) {
        context.signal?.throwIfAborted();
        const queuePath = resolveQueuePath(config);
        const defaultDelay = config.defaultRetryDelayMs ?? 60_000;
        let state = await loadState(queuePath);

        if (params.status === "sent") {
          state = markSent(state, params.entryId, params.lockId);
          await saveState(queuePath, state);
          return { acked: true, status: "sent", entryId: params.entryId };
        }

        const retryAt = params.error
          ? parseRetryAfter(params.error, defaultDelay)
          : new Date(Date.now() + defaultDelay).toISOString();

        state = markFailed(state, params.entryId, params.lockId, params.error ?? "unknown", retryAt);
        await saveState(queuePath, state);

        const entry = state.entries.find((e) => e.id === params.entryId);
        const isFinalFailure = entry?.status === "failed";

        return {
          acked: true,
          status: isFinalFailure ? "failed_permanently" : "requeued",
          retryCount: entry?.retryCount ?? 0,
          rateLimitUntil: retryAt,
          entryId: params.entryId,
          instruction: isFinalFailure
            ? "Max retries exceeded. Notify the user that this message could not be delivered."
            : `Requeued for retry. Schedule a cron wake at ${retryAt}.`,
        };
      },
    }),

    tool({
      name: "mq_status",
      label: "Queue Status",
      description:
        "Return current queue state: counts by status, next retry time, oldest pending entry.",
      parameters: Type.Object({}),
      async execute(_params, config, context) {
        context.signal?.throwIfAborted();
        const queuePath = resolveQueuePath(config);
        let state = await loadState(queuePath);
        state = releaseExpiredLocks(state);

        const counts: Record<string, number> = {};
        for (const e of state.entries) {
          counts[e.status] = (counts[e.status] ?? 0) + 1;
        }

        const pending = state.entries.filter((e) => e.status === "pending");
        const nextRetry = pending
          .filter((e) => e.rateLimitUntil)
          .sort((a, b) => a.rateLimitUntil.localeCompare(b.rateLimitUntil))[0];

        const ready = pendingReadyEntries(state);

        return {
          total: state.entries.length,
          counts,
          readyNow: ready.length,
          nextRetryAt: nextRetry?.rateLimitUntil ?? null,
          lastProcessedAt: state.lastProcessedAt,
          oldestPending: pending[0]
            ? {
                id: pending[0].id,
                channel: pending[0].channel,
                sender: pending[0].sender,
                queuedAt: pending[0].queuedAt,
                retryCount: pending[0].retryCount,
              }
            : null,
        };
      },
    }),
  ],
});

function resolveQueuePath(config: { queuePath?: string }): string {
  return (
    config.queuePath ??
    join(process.env.OPENCLAW_WORKSPACE ?? process.cwd(), "message-queue-state.json")
  );
}
