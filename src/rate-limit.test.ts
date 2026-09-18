import { describe, it, expect } from "vitest";
import { isRateLimitError, parseRetryAfter } from "./rate-limit.js";

describe("isRateLimitError", () => {
  it.each([
    "429 Too Many Requests",
    "Rate limit exceeded",
    "rate_limit",
    "RATE_LIMITED",
    "throttled: please wait",
    "quota exceeded for this API",
    "retry-after: 60",
  ])("detects '%s' as rate limit", (error) => {
    expect(isRateLimitError(error)).toBe(true);
  });

  it.each([
    "Connection refused",
    "500 Internal Server Error",
    "ENOTFOUND",
    "Authentication failed",
  ])("rejects '%s' as not rate limit", (error) => {
    expect(isRateLimitError(error)).toBe(false);
  });
});

describe("parseRetryAfter", () => {
  it("parses 'retry-after: 30' as 30s from now", () => {
    const before = Date.now();
    const result = new Date(parseRetryAfter("retry-after: 30", 60_000)).getTime();
    expect(result).toBeGreaterThanOrEqual(before + 29_000);
    expect(result).toBeLessThanOrEqual(before + 31_000);
  });

  it("parses 'retry after 120 seconds'", () => {
    const before = Date.now();
    const result = new Date(parseRetryAfter("Please retry after 120 seconds", 60_000)).getTime();
    expect(result).toBeGreaterThanOrEqual(before + 119_000);
    expect(result).toBeLessThanOrEqual(before + 121_000);
  });

  it("parses ISO timestamp", () => {
    const result = parseRetryAfter(
      "Rate limited. Resets at 2026-09-18T12:00:00Z",
      60_000,
    );
    expect(result).toBe("2026-09-18T12:00:00.000Z");
  });

  it("falls back to default when unparseable", () => {
    const before = Date.now();
    const result = new Date(parseRetryAfter("429 Too Many Requests", 60_000)).getTime();
    expect(result).toBeGreaterThanOrEqual(before + 59_000);
    expect(result).toBeLessThanOrEqual(before + 61_000);
  });
});
