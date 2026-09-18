const RATE_LIMIT_PATTERNS = [
  /rate.?limit/i,
  /429/,
  /too many requests/i,
  /throttl/i,
  /quota exceeded/i,
  /RATE_LIMITED/,
  /retry.?after/i,
] as const;

const RETRY_AFTER_PATTERNS = [
  /retry[- ]?after:\s*(\d+)/i,
  /retry after (\d+)\s*s/i,
  /wait (\d+)\s*second/i,
  /try again in (\d+)/i,
  /reset(?:s)? (?:at|in) (\d{4}-\d{2}-\d{2}T[\d:.]+Z?)/i,
] as const;

export function isRateLimitError(error: string): boolean {
  return RATE_LIMIT_PATTERNS.some((p) => p.test(error));
}

export function parseRetryAfter(error: string, defaultDelayMs: number): string {
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

  return new Date(Date.now() + defaultDelayMs).toISOString();
}
