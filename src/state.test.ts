import { describe, it, expect } from "vitest";
import {
  emptyState,
  enqueue,
  acquireLock,
  markSent,
  markFailed,
  releaseExpiredLocks,
  pruneCompleted,
  pendingReadyEntries,
  canTransition,
  dedupeKey,
} from "./state.js";

describe("state transitions", () => {
  it("allows pending → processing", () => {
    expect(canTransition("pending", "processing")).toBe(true);
  });

  it("allows processing → sent", () => {
    expect(canTransition("processing", "sent")).toBe(true);
  });

  it("allows processing → failed", () => {
    expect(canTransition("processing", "failed")).toBe(true);
  });

  it("allows processing → pending (retry)", () => {
    expect(canTransition("processing", "pending")).toBe(true);
  });

  it("disallows sent → pending", () => {
    expect(canTransition("sent", "pending")).toBe(false);
  });

  it("disallows pending → sent (must go through processing)", () => {
    expect(canTransition("pending", "sent")).toBe(false);
  });
});

describe("enqueue", () => {
  it("adds a new entry", () => {
    const state = emptyState();
    const result = enqueue(state, {
      ts: "2026-09-18T10:00:00Z",
      channel: "whatsapp",
      chatId: "+1234567890",
      sender: "+1234567890",
      messageText: "Hello",
      rateLimitError: "429 Too Many Requests",
      rateLimitUntil: "2026-09-18T10:01:00Z",
      retryCount: 0,
    });

    expect(result.alreadyExists).toBe(false);
    expect(result.state.entries).toHaveLength(1);
    expect(result.state.entries[0].status).toBe("pending");
    expect(result.state.entries[0].id).toBe(result.entryId);
  });

  it("deduplicates identical messages", () => {
    const state = emptyState();
    const entry = {
      ts: "2026-09-18T10:00:00Z",
      channel: "whatsapp",
      chatId: "+1234567890",
      sender: "+1234567890",
      messageText: "Hello",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T10:01:00Z",
      retryCount: 0,
    };

    const first = enqueue(state, entry);
    const second = enqueue(first.state, entry);

    expect(second.alreadyExists).toBe(true);
    expect(second.state.entries).toHaveLength(1);
    expect(second.entryId).toBe(first.entryId);
  });

  it("distinguishes messages with different timestamps", () => {
    const state = emptyState();
    const base = {
      channel: "whatsapp",
      chatId: "+1234567890",
      sender: "+1234567890",
      messageText: "Hello",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T10:01:00Z",
      retryCount: 0,
    };

    const first = enqueue(state, { ...base, ts: "2026-09-18T10:00:00Z" });
    const second = enqueue(first.state, { ...base, ts: "2026-09-18T10:05:00Z" });

    expect(second.alreadyExists).toBe(false);
    expect(second.state.entries).toHaveLength(2);
  });
});

describe("locking", () => {
  function queuedState() {
    const state = emptyState();
    return enqueue(state, {
      ts: "2026-09-18T10:00:00Z",
      channel: "telegram",
      chatId: "123",
      sender: "user",
      messageText: "test",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T09:00:00Z",
      retryCount: 0,
    });
  }

  it("acquires lock on pending entry", () => {
    const { state, entryId } = queuedState();
    const result = acquireLock(state, entryId, "lock-1");
    expect(result.acquired).toBe(true);
    expect(result.state.entries[0].status).toBe("processing");
    expect(result.state.entries[0].lockedBy).toBe("lock-1");
  });

  it("rejects second lock on same entry", () => {
    const { state, entryId } = queuedState();
    const first = acquireLock(state, entryId, "lock-1");
    const second = acquireLock(first.state, entryId, "lock-2");
    expect(second.acquired).toBe(false);
  });

  it("allows lock after expiry", () => {
    const { state, entryId } = queuedState();
    const locked = acquireLock(state, entryId, "lock-1", 0);
    const released = releaseExpiredLocks(locked.state);
    expect(released.entries[0].status).toBe("pending");
    const reLocked = acquireLock(released, entryId, "lock-2");
    expect(reLocked.acquired).toBe(true);
  });
});

describe("markSent", () => {
  it("transitions processing → sent with matching lock", () => {
    const { state, entryId } = enqueue(emptyState(), {
      ts: "2026-09-18T10:00:00Z",
      channel: "imessage",
      chatId: "80",
      sender: "+1306",
      messageText: "hi",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T09:00:00Z",
      retryCount: 0,
    });

    const locked = acquireLock(state, entryId, "lock-1");
    const sent = markSent(locked.state, entryId, "lock-1");
    expect(sent.entries[0].status).toBe("sent");
    expect(sent.entries[0].sentAt).not.toBeNull();
  });

  it("rejects wrong lock", () => {
    const { state, entryId } = enqueue(emptyState(), {
      ts: "2026-09-18T10:00:00Z",
      channel: "imessage",
      chatId: "80",
      sender: "+1306",
      messageText: "hi",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T09:00:00Z",
      retryCount: 0,
    });

    const locked = acquireLock(state, entryId, "lock-1");
    const sent = markSent(locked.state, entryId, "wrong-lock");
    expect(sent.entries[0].status).toBe("processing");
  });
});

describe("markFailed", () => {
  it("requeues under max retries", () => {
    const { state, entryId } = enqueue(emptyState(), {
      ts: "2026-09-18T10:00:00Z",
      channel: "discord",
      chatId: "ch1",
      sender: "user",
      messageText: "msg",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T09:00:00Z",
      retryCount: 0,
    });

    const locked = acquireLock(state, entryId, "lock-1");
    const failed = markFailed(locked.state, entryId, "lock-1", "429 again", "2026-09-18T10:05:00Z");
    expect(failed.entries[0].status).toBe("pending");
    expect(failed.entries[0].retryCount).toBe(1);
  });

  it("marks failed after 5 retries", () => {
    let state = emptyState();
    const result = enqueue(state, {
      ts: "2026-09-18T10:00:00Z",
      channel: "signal",
      chatId: "ch1",
      sender: "user",
      messageText: "msg",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T09:00:00Z",
      retryCount: 0,
    });
    state = result.state;
    const entryId = result.entryId;

    for (let i = 0; i < 5; i++) {
      const locked = acquireLock(state, entryId, `lock-${i}`);
      state = markFailed(locked.state, entryId, `lock-${i}`, "429", null);
    }

    expect(state.entries[0].status).toBe("failed");
    expect(state.entries[0].retryCount).toBe(5);
  });
});

describe("pruneCompleted", () => {
  it("removes old sent entries", () => {
    const state = emptyState();
    const result = enqueue(state, {
      ts: "2026-09-17T10:00:00Z",
      channel: "whatsapp",
      chatId: "ch1",
      sender: "user",
      messageText: "old",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-17T10:01:00Z",
      retryCount: 0,
    });

    const locked = acquireLock(result.state, result.entryId, "lock-1");
    let sent = markSent(locked.state, result.entryId, "lock-1");
    sent.entries[0].sentAt = "2026-09-16T10:00:00Z";

    const pruned = pruneCompleted(sent, 86_400_000);
    expect(pruned.entries).toHaveLength(0);
  });

  it("keeps pending entries", () => {
    const { state } = enqueue(emptyState(), {
      ts: "2026-09-18T10:00:00Z",
      channel: "whatsapp",
      chatId: "ch1",
      sender: "user",
      messageText: "pending",
      rateLimitError: "429",
      rateLimitUntil: "2026-09-18T10:01:00Z",
      retryCount: 0,
    });

    const pruned = pruneCompleted(state);
    expect(pruned.entries).toHaveLength(1);
  });
});

describe("pendingReadyEntries", () => {
  it("returns entries past their rate limit window", () => {
    const { state } = enqueue(emptyState(), {
      ts: "2026-09-18T10:00:00Z",
      channel: "telegram",
      chatId: "ch1",
      sender: "user",
      messageText: "ready",
      rateLimitError: "429",
      rateLimitUntil: "2020-01-01T00:00:00Z",
      retryCount: 0,
    });

    const ready = pendingReadyEntries(state);
    expect(ready).toHaveLength(1);
  });

  it("skips entries still rate-limited", () => {
    const { state } = enqueue(emptyState(), {
      ts: "2026-09-18T10:00:00Z",
      channel: "telegram",
      chatId: "ch1",
      sender: "user",
      messageText: "not ready",
      rateLimitError: "429",
      rateLimitUntil: "2099-01-01T00:00:00Z",
      retryCount: 0,
    });

    const ready = pendingReadyEntries(state);
    expect(ready).toHaveLength(0);
  });
});
