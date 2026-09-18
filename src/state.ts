import { readFile, writeFile, rename } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";

// ── Domain types ────────────────────────────────────────────────────────

export type MessageStatus = "pending" | "processing" | "sent" | "failed";

export interface QueueEntry {
  id: string;
  dedupeKey: string;
  ts: string;
  channel: string;
  chatId: string;
  sender: string;
  messageText: string;
  rateLimitError: string;
  rateLimitUntil: string;
  retryCount: number;
  status: MessageStatus;
  queuedAt: string;
  sentAt: string | null;
  lastAttemptAt: string | null;
  lockedBy: string | null;
  lockExpiresAt: string | null;
}

export interface QueueState {
  version: 1;
  entries: QueueEntry[];
  lastProcessedAt: string | null;
}

// ── State transitions (the only valid moves) ────────────────────────────

const VALID_TRANSITIONS: Record<MessageStatus, MessageStatus[]> = {
  pending: ["processing"],
  processing: ["sent", "failed", "pending"],
  sent: [],
  failed: ["pending"],
};

export function canTransition(from: MessageStatus, to: MessageStatus): boolean {
  return VALID_TRANSITIONS[from].includes(to);
}

// ── Atomic file operations ──────────────────────────────────────────────

export function emptyState(): QueueState {
  return { version: 1, entries: [], lastProcessedAt: null };
}

export async function loadState(path: string): Promise<QueueState> {
  if (!existsSync(path)) return emptyState();
  const raw = await readFile(path, "utf-8");
  if (!raw.trim()) return emptyState();
  const parsed = JSON.parse(raw) as QueueState;
  if (parsed.version !== 1) throw new Error(`Unsupported queue state version: ${parsed.version}`);
  return parsed;
}

export async function saveState(path: string, state: QueueState): Promise<void> {
  const tmp = path + `.tmp-${randomUUID().slice(0, 8)}`;
  const dir = dirname(path);
  await writeFile(tmp, JSON.stringify(state, null, 2) + "\n", "utf-8");
  await rename(tmp, path);
}

// ── Deduplication ───────────────────────────────────────────────────────

export function dedupeKey(channel: string, chatId: string, messageText: string, ts: string): string {
  return `${channel}:${chatId}:${ts}:${simpleHash(messageText)}`;
}

function simpleHash(s: string): string {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  }
  return (h >>> 0).toString(36);
}

// ── Queue operations (all idempotent) ───────────────────────────────────

export function enqueue(
  state: QueueState,
  entry: Omit<QueueEntry, "id" | "dedupeKey" | "status" | "queuedAt" | "sentAt" | "lastAttemptAt" | "lockedBy" | "lockExpiresAt">,
): { state: QueueState; entryId: string; alreadyExists: boolean } {
  const key = dedupeKey(entry.channel, entry.chatId, entry.messageText, entry.ts);
  const existing = state.entries.find((e) => e.dedupeKey === key);
  if (existing) {
    return { state, entryId: existing.id, alreadyExists: true };
  }

  const id = randomUUID();
  const newEntry: QueueEntry = {
    ...entry,
    id,
    dedupeKey: key,
    status: "pending",
    queuedAt: new Date().toISOString(),
    sentAt: null,
    lastAttemptAt: null,
    lockedBy: null,
    lockExpiresAt: null,
  };

  return {
    state: { ...state, entries: [...state.entries, newEntry] },
    entryId: id,
    alreadyExists: false,
  };
}

export function acquireLock(
  state: QueueState,
  entryId: string,
  lockId: string,
  lockDurationMs: number = 120_000,
): { state: QueueState; acquired: boolean } {
  const idx = state.entries.findIndex((e) => e.id === entryId);
  if (idx === -1) return { state, acquired: false };

  const entry = state.entries[idx];
  if (entry.status !== "pending") return { state, acquired: false };

  const now = new Date();
  if (entry.lockedBy && entry.lockExpiresAt && new Date(entry.lockExpiresAt) > now) {
    return { state, acquired: false };
  }

  const updated = { ...entry };
  updated.status = "processing" as const;
  updated.lockedBy = lockId;
  updated.lockExpiresAt = new Date(now.getTime() + lockDurationMs).toISOString();
  updated.lastAttemptAt = now.toISOString();

  const entries = [...state.entries];
  entries[idx] = updated;
  return { state: { ...state, entries }, acquired: true };
}

export function markSent(state: QueueState, entryId: string, lockId: string): QueueState {
  const idx = state.entries.findIndex((e) => e.id === entryId);
  if (idx === -1) return state;

  const entry = state.entries[idx];
  if (entry.status !== "processing" || entry.lockedBy !== lockId) return state;

  const entries = [...state.entries];
  entries[idx] = {
    ...entry,
    status: "sent" as const,
    sentAt: new Date().toISOString(),
    lockedBy: null,
    lockExpiresAt: null,
  };
  return { ...state, entries };
}

export function markFailed(
  state: QueueState,
  entryId: string,
  lockId: string,
  error: string,
  retryAt: string | null,
): QueueState {
  const idx = state.entries.findIndex((e) => e.id === entryId);
  if (idx === -1) return state;

  const entry = state.entries[idx];
  if (entry.status !== "processing" || entry.lockedBy !== lockId) return state;

  const entries = [...state.entries];
  const nextRetry = entry.retryCount + 1;
  entries[idx] = {
    ...entry,
    status: nextRetry >= 5 ? ("failed" as const) : ("pending" as const),
    retryCount: nextRetry,
    rateLimitError: error,
    rateLimitUntil: retryAt ?? entry.rateLimitUntil,
    lockedBy: null,
    lockExpiresAt: null,
  };
  return { ...state, entries };
}

export function releaseExpiredLocks(state: QueueState): QueueState {
  const now = new Date();
  let changed = false;
  const entries = state.entries.map((entry) => {
    if (
      entry.status === "processing" &&
      entry.lockExpiresAt &&
      new Date(entry.lockExpiresAt) <= now
    ) {
      changed = true;
      return { ...entry, status: "pending" as const, lockedBy: null, lockExpiresAt: null };
    }
    return entry;
  });
  return changed ? { ...state, entries } : state;
}

export function pruneCompleted(state: QueueState, maxAgeMs: number = 86_400_000): QueueState {
  const cutoff = new Date(Date.now() - maxAgeMs);
  const entries = state.entries.filter((e) => {
    if (e.status !== "sent") return true;
    return e.sentAt ? new Date(e.sentAt) > cutoff : true;
  });
  return { ...state, entries, lastProcessedAt: new Date().toISOString() };
}

export function pendingReadyEntries(state: QueueState): QueueEntry[] {
  const now = new Date();
  return state.entries.filter(
    (e) =>
      e.status === "pending" &&
      (!e.rateLimitUntil || new Date(e.rateLimitUntil) <= now) &&
      (!e.lockedBy || !e.lockExpiresAt || new Date(e.lockExpiresAt) <= now),
  );
}
