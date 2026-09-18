#!/usr/bin/env node

/**
 * Message queue CLI for rate-limit recovery.
 * Usage: node scripts/mq.mjs <command> [options]
 *
 * Commands:
 *   enqueue   Queue a message that failed due to a rate limit
 *   process   Lock and list ready messages for processing
 *   ack       Mark a message as sent or retry
 *   status    Show queue counts and next retry time
 *   prune     Remove completed entries older than 24h
 */

import { readFile, writeFile, rename } from "node:fs/promises";
import { existsSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { join, dirname } from "node:path";
import { parseArgs } from "node:util";

// ── Config ──────────────────────────────────────────────────────────────

const QUEUE_PATH = process.env.MQ_QUEUE_PATH ??
  join(process.env.OPENCLAW_WORKSPACE ?? process.cwd(), "message-queue-state.json");
const MAX_RETRIES = 5;
const DEFAULT_RETRY_DELAY_MS = 60_000;
const LOCK_DURATION_MS = 120_000;

// ── State types and helpers ─────────────────────────────────────────────

function emptyState() {
  return { version: 1, entries: [], lastProcessedAt: null };
}

async function loadState() {
  if (!existsSync(QUEUE_PATH)) return emptyState();
  const raw = await readFile(QUEUE_PATH, "utf-8");
  if (!raw.trim()) return emptyState();
  return JSON.parse(raw);
}

async function saveState(state) {
  const tmp = QUEUE_PATH + `.tmp-${randomUUID().slice(0, 8)}`;
  await writeFile(tmp, JSON.stringify(state, null, 2) + "\n", "utf-8");
  await rename(tmp, QUEUE_PATH);
}

function contentHash(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  }
  return (h >>> 0).toString(36);
}

function makeDedupeKey(channel, chatId, messageText, ts) {
  return `${channel}:${chatId}:${ts}:${contentHash(messageText)}`;
}

// ── Rate limit parsing ──────────────────────────────────────────────────

const RATE_LIMIT_PATTERNS = [
  /rate.?limit/i, /429/, /too many requests/i,
  /throttl/i, /quota exceeded/i, /RATE_LIMITED/, /retry.?after/i,
];

const RETRY_AFTER_PATTERNS = [
  /retry[- ]?after:\s*(\d+)/i,
  /retry after (\d+)\s*s/i,
  /wait (\d+)\s*second/i,
  /try again in (\d+)/i,
  /reset(?:s)? (?:at|in) (\d{4}-\d{2}-\d{2}T[\d:.]+Z?)/i,
];

function isRateLimitError(error) {
  return RATE_LIMIT_PATTERNS.some((p) => p.test(error));
}

function parseRetryAfter(error) {
  for (const pattern of RETRY_AFTER_PATTERNS) {
    const match = error.match(pattern);
    if (!match?.[1]) continue;
    if (match[1].includes("-")) {
      const parsed = new Date(match[1]);
      if (!isNaN(parsed.getTime())) return parsed.toISOString();
    }
    const seconds = parseInt(match[1], 10);
    if (!isNaN(seconds) && seconds > 0 && seconds < 86400) {
      return new Date(Date.now() + seconds * 1000).toISOString();
    }
  }
  return new Date(Date.now() + DEFAULT_RETRY_DELAY_MS).toISOString();
}

// ── Commands ────────────────────────────────────────────────────────────

async function cmdEnqueue(args) {
  const { values } = parseArgs({
    args,
    options: {
      channel: { type: "string" },
      "chat-id": { type: "string" },
      sender: { type: "string" },
      message: { type: "string" },
      ts: { type: "string" },
      error: { type: "string" },
    },
    strict: true,
  });

  const { channel, sender, message, ts, error } = values;
  const chatId = values["chat-id"];

  if (!channel || !chatId || !sender || !message || !ts || !error) {
    console.error("Missing required flags. Need: --channel --chat-id --sender --message --ts --error");
    process.exit(1);
  }

  if (!isRateLimitError(error)) {
    console.log(JSON.stringify({ enqueued: false, reason: "Not a rate-limit error" }));
    return;
  }

  const retryAt = parseRetryAfter(error);
  let state = await loadState();
  state = releaseExpiredLocks(state);

  const key = makeDedupeKey(channel, chatId, message, ts);
  const existing = state.entries.find((e) => e.dedupeKey === key);

  if (existing) {
    console.log(JSON.stringify({ enqueued: false, alreadyExists: true, entryId: existing.id }));
    return;
  }

  const id = randomUUID();
  state.entries.push({
    id,
    dedupeKey: key,
    ts,
    channel,
    chatId,
    sender,
    messageText: message,
    rateLimitError: error,
    rateLimitUntil: retryAt,
    retryCount: 0,
    status: "pending",
    queuedAt: new Date().toISOString(),
    sentAt: null,
    lastAttemptAt: null,
    lockedBy: null,
    lockExpiresAt: null,
  });

  await saveState(state);
  console.log(JSON.stringify({ enqueued: true, entryId: id, rateLimitUntil: retryAt }));
}

async function cmdProcess() {
  let state = await loadState();
  state = releaseExpiredLocks(state);

  const now = new Date();
  const ready = state.entries.filter(
    (e) =>
      e.status === "pending" &&
      (!e.rateLimitUntil || new Date(e.rateLimitUntil) <= now) &&
      (!e.lockedBy || !e.lockExpiresAt || new Date(e.lockExpiresAt) <= now),
  );

  if (ready.length === 0) {
    state = pruneOld(state);
    await saveState(state);
    console.log(JSON.stringify({ processed: 0, message: "No messages ready." }));
    return;
  }

  const lockId = randomUUID();
  const locked = [];

  for (const entry of ready) {
    entry.status = "processing";
    entry.lockedBy = lockId;
    entry.lockExpiresAt = new Date(now.getTime() + LOCK_DURATION_MS).toISOString();
    entry.lastAttemptAt = now.toISOString();
    locked.push({
      id: entry.id,
      channel: entry.channel,
      chatId: entry.chatId,
      sender: entry.sender,
      messagePreview: entry.messageText.slice(0, 200),
    });
  }

  state = pruneOld(state);
  await saveState(state);
  console.log(JSON.stringify({ processed: locked.length, lockId, entries: locked }));
}

async function cmdAck(args) {
  const { values } = parseArgs({
    args,
    options: {
      id: { type: "string" },
      lock: { type: "string" },
      status: { type: "string" },
      error: { type: "string" },
    },
    strict: true,
  });

  if (!values.id || !values.lock || !values.status) {
    console.error("Missing required flags. Need: --id --lock --status (sent|retry)");
    process.exit(1);
  }

  let state = await loadState();
  const entry = state.entries.find((e) => e.id === values.id);

  if (!entry || entry.status !== "processing" || entry.lockedBy !== values.lock) {
    console.log(JSON.stringify({ acked: false, reason: "Entry not found, wrong status, or wrong lock" }));
    return;
  }

  if (values.status === "sent") {
    entry.status = "sent";
    entry.sentAt = new Date().toISOString();
    entry.lockedBy = null;
    entry.lockExpiresAt = null;
    await saveState(state);
    console.log(JSON.stringify({ acked: true, status: "sent" }));
    return;
  }

  const retryAt = values.error ? parseRetryAfter(values.error) : new Date(Date.now() + DEFAULT_RETRY_DELAY_MS).toISOString();
  entry.retryCount += 1;
  entry.rateLimitError = values.error ?? "unknown";
  entry.rateLimitUntil = retryAt;
  entry.lockedBy = null;
  entry.lockExpiresAt = null;
  entry.status = entry.retryCount >= MAX_RETRIES ? "failed" : "pending";

  await saveState(state);
  console.log(JSON.stringify({
    acked: true,
    status: entry.status === "failed" ? "failed_permanently" : "requeued",
    retryCount: entry.retryCount,
    rateLimitUntil: retryAt,
  }));
}

async function cmdStatus() {
  let state = await loadState();
  state = releaseExpiredLocks(state);

  const counts = {};
  for (const e of state.entries) counts[e.status] = (counts[e.status] ?? 0) + 1;

  const pending = state.entries.filter((e) => e.status === "pending");
  const now = new Date();
  const readyNow = pending.filter(
    (e) => !e.rateLimitUntil || new Date(e.rateLimitUntil) <= now,
  ).length;

  const nextRetry = pending
    .filter((e) => e.rateLimitUntil)
    .sort((a, b) => a.rateLimitUntil.localeCompare(b.rateLimitUntil))[0];

  console.log(JSON.stringify({
    total: state.entries.length,
    counts,
    readyNow,
    nextRetryAt: nextRetry?.rateLimitUntil ?? null,
    lastProcessedAt: state.lastProcessedAt,
  }));
}

async function cmdPrune() {
  let state = await loadState();
  state = pruneOld(state);
  await saveState(state);
  console.log(JSON.stringify({ remaining: state.entries.length }));
}

// ── Helpers ─────────────────────────────────────────────────────────────

function releaseExpiredLocks(state) {
  const now = new Date();
  for (const entry of state.entries) {
    if (entry.status === "processing" && entry.lockExpiresAt && new Date(entry.lockExpiresAt) <= now) {
      entry.status = "pending";
      entry.lockedBy = null;
      entry.lockExpiresAt = null;
    }
  }
  return state;
}

function pruneOld(state) {
  const cutoff = new Date(Date.now() - 86_400_000);
  state.entries = state.entries.filter((e) => {
    if (e.status !== "sent") return true;
    return e.sentAt ? new Date(e.sentAt) > cutoff : true;
  });
  state.lastProcessedAt = new Date().toISOString();
  return state;
}

// ── Main ────────────────────────────────────────────────────────────────

const [command, ...rest] = process.argv.slice(2);

switch (command) {
  case "enqueue": await cmdEnqueue(rest); break;
  case "process": await cmdProcess(); break;
  case "ack": await cmdAck(rest); break;
  case "status": await cmdStatus(); break;
  case "prune": await cmdPrune(); break;
  default:
    console.error("Usage: node scripts/mq.mjs <enqueue|process|ack|status|prune> [options]");
    process.exit(1);
}
