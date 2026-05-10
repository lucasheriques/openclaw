import fs from "node:fs/promises";
import path from "node:path";

// Sentinel files written by media-producing tools (e.g. ngr-analysis's
// `save()`) alongside their output. When an agent forgets to call
// `message(media=...)`, the rescue path scans these sentinels and
// attaches the associated file to the outgoing payload so the file
// still ships.
//
// Contract:
//  - Sentinel: `<output>.auto-attach`, JSON `{output: <path>, created_at: <epoch-seconds>}`
//  - Consumed exactly once: whichever path (explicit send or rescue)
//    delivers the file is also responsible for removing the sentinel.
//    Double-send prevention hinges on this invariant.
//  - Time-windowed: only sentinels younger than MAX_AGE_MS are rescued,
//    so abandoned files from prior sessions don't resurrect themselves.

const SENTINEL_SUFFIX = ".auto-attach";
const MAX_AGE_MS = 5 * 60_000;

type SentinelPayload = {
  output: string;
  created_at: number;
};

type RescuedMedia = {
  path: string;
  sentinel: string;
};

// rescueOrphanMedia scans the provided roots for recent sentinels and
// returns the associated file paths. Each returned entry carries its
// sentinel path; callers must delete the sentinel via consumeSentinel
// after the file ships (success OR deliberate drop) so rescues stay
// single-shot.
export async function rescueOrphanMedia(
  roots: readonly string[] | undefined,
): Promise<RescuedMedia[]> {
  if (!roots || roots.length === 0) {
    return [];
  }

  const rescued: RescuedMedia[] = [];
  const now = Date.now();

  for (const root of roots) {
    const entries = await readDirSafe(root);
    for (const entry of entries) {
      if (!entry.endsWith(SENTINEL_SUFFIX)) {
        continue;
      }
      const sentinelPath = path.join(root, entry);
      const parsed = await readSentinel(sentinelPath);
      if (!parsed) {
        continue;
      }
      const ageMs = now - parsed.created_at * 1000;
      if (ageMs > MAX_AGE_MS || ageMs < -30_000) {
        // Skip stale sentinels and anything suspiciously in the future
        // (system clock drift). The 30s future tolerance absorbs small
        // skews without rescuing files that shouldn't exist yet.
        continue;
      }
      const filePath = parsed.output;
      try {
        await fs.access(filePath);
      } catch {
        // Sentinel points at a missing file — ignore and let the caller
        // garbage-collect on the next sweep.
        continue;
      }
      rescued.push({ path: filePath, sentinel: sentinelPath });
    }
  }

  return rescued;
}

// consumeSentinel deletes a sentinel after the associated file has been
// shipped (or deliberately dropped). Best-effort: a failure here only
// risks a duplicate rescue on the next turn, not a data corruption.
export async function consumeSentinel(sentinelPath: string): Promise<void> {
  try {
    await fs.unlink(sentinelPath);
  } catch {
    // Already gone (raced with another consumer) — fine.
  }
}

// consumeSentinelForFile is the helper used when a file is shipped via
// the normal `message(media=...)` path. We don't have the sentinel path
// handy there, so we compute it from the file path and try to delete.
export async function consumeSentinelForFile(filePath: string): Promise<void> {
  await consumeSentinel(filePath + SENTINEL_SUFFIX);
}

async function readDirSafe(root: string): Promise<string[]> {
  try {
    return await fs.readdir(root);
  } catch {
    return [];
  }
}

async function readSentinel(sentinelPath: string): Promise<SentinelPayload | null> {
  try {
    const raw = await fs.readFile(sentinelPath, "utf8");
    const parsed = JSON.parse(raw) as Partial<SentinelPayload>;
    if (typeof parsed.output !== "string" || typeof parsed.created_at !== "number") {
      return null;
    }
    return { output: parsed.output, created_at: parsed.created_at };
  } catch {
    return null;
  }
}
