import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { setupCronServiceSuite, writeCronStoreSnapshot } from "../../cron/service.test-harness.js";
import { createCronServiceState } from "../../cron/service/state.js";
import { onTimer } from "../../cron/service/timer.js";
import { loadCronStore, saveCronStore } from "../../cron/store.js";
import type { CronJob } from "../../cron/types.js";
import * as detachedTaskRuntime from "../../tasks/detached-task-runtime.js";
import { findTaskByRunId, resetTaskRegistryForTests } from "../../tasks/task-registry.js";

const { logger, makeStorePath } = setupCronServiceSuite({
  prefix: "cron-service-timer-seam",
});

function createDueMainJob(params: { now: number; wakeMode: CronJob["wakeMode"] }): CronJob {
  return {
    id: "main-heartbeat-job",
    name: "main heartbeat job",
    enabled: true,
    createdAtMs: params.now - 60_000,
    updatedAtMs: params.now - 60_000,
    schedule: { kind: "every", everyMs: 60_000, anchorMs: params.now - 60_000 },
    sessionTarget: "main",
    wakeMode: params.wakeMode,
    payload: { kind: "systemEvent", text: "heartbeat seam tick" },
    sessionKey: "agent:main:main",
    state: { nextRunAtMs: params.now - 1 },
  };
}

afterEach(() => {
  resetTaskRegistryForTests();
  vi.unstubAllEnvs();
});

describe("cron service timer seam coverage", () => {
  it("pauses Gringo WhatsApp one-shot cron jobs without running the agent when access is inactive", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-05-20T23:15:00.000Z");
    const ngrBin = `${storePath}.ngr-test`;
    await fs.mkdir(path.dirname(ngrBin), { recursive: true });
    await fs.writeFile(
      ngrBin,
      [
        "#!/bin/sh",
        'printf \'%s\\n\' \'{"eligible":false,"reason":"free_or_inactive","hasAccess":false}\'',
      ].join("\n"),
      "utf8",
    );
    await fs.chmod(ngrBin, 0o755);
    vi.stubEnv("GRINGO_NGR_BIN", ngrBin);

    const runIsolatedAgentJob = vi.fn(async () => ({ status: "ok" as const }));
    await writeCronStoreSnapshot({
      storePath,
      jobs: [
        {
          id: "gringo-interview-reminder",
          name: "gringo interview reminder",
          agentId: "gringo",
          enabled: true,
          createdAtMs: now - 60_000,
          updatedAtMs: now - 60_000,
          schedule: { kind: "at", at: new Date(now - 1_000).toISOString() },
          sessionTarget: "isolated",
          wakeMode: "now",
          payload: { kind: "agentTurn", message: "Remind Lucas to prep for the interview." },
          sessionKey: "agent:gringo:whatsapp:direct:+5511999999999",
          delivery: { mode: "announce", channel: "whatsapp", to: "+5511999999999" },
          deleteAfterRun: true,
          state: { nextRunAtMs: now - 1_000 },
        },
      ],
    });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent: vi.fn(),
      requestHeartbeat: vi.fn(),
      runIsolatedAgentJob,
    });

    await onTimer(state);

    expect(runIsolatedAgentJob).not.toHaveBeenCalled();
    const persisted = await loadCronStore(storePath);
    const job = persisted.jobs[0];
    expect(job?.enabled).toBe(true);
    expect(job?.state.lastStatus).toBe("skipped");
    expect(job?.state.lastError).toBe("gringo-access-free_or_inactive");
    expect(job?.state.nextRunAtMs).toBe(now + 24 * 60 * 60_000);
    expect(job?.state.runningAtMs).toBeUndefined();
  });

  it("persists the next schedule and hands off next-heartbeat main jobs", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const timeoutSpy = vi.spyOn(globalThis, "setTimeout");

    await writeCronStoreSnapshot({
      storePath,
      jobs: [createDueMainJob({ now, wakeMode: "next-heartbeat" })],
    });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
    });

    await onTimer(state);

    expect(enqueueSystemEvent).toHaveBeenCalledWith("heartbeat seam tick", {
      agentId: undefined,
      sessionKey: "agent:main:main",
      contextKey: "cron:main-heartbeat-job",
    });
    expect(requestHeartbeat).toHaveBeenCalledWith({
      source: "cron",
      intent: "event",
      reason: "cron:main-heartbeat-job",
      agentId: undefined,
      sessionKey: "agent:main:main",
      heartbeat: { target: "last" },
    });

    const persisted = await loadCronStore(storePath);
    const job = persisted.jobs[0];
    if (!job) {
      throw new Error("expected persisted heartbeat cron job");
    }
    expect(job.state.lastStatus).toBe("ok");
    expect(job.state.runningAtMs).toBeUndefined();
    expect(job.state.nextRunAtMs).toBe(now + 60_000);
    const task = findTaskByRunId(`cron:main-heartbeat-job:${now}`);
    if (!task) {
      throw new Error("expected cron task ledger record");
    }
    expect(task.runtime).toBe("cron");
    expect(task.sourceId).toBe("main-heartbeat-job");
    expect(task.ownerKey).toBe("");
    expect(task.scopeKind).toBe("system");
    expect(task.childSessionKey).toBe("agent:main:main");
    expect(task.runId).toBe(`cron:main-heartbeat-job:${now}`);
    expect(task.label).toBe("main heartbeat job");
    expect(task.task).toBe("main heartbeat job");
    expect(task.status).toBe("succeeded");
    expect(task.deliveryStatus).toBe("not_applicable");
    expect(task.notifyPolicy).toBe("silent");
    expect(task.startedAt).toBe(now);
    expect(task.lastEventAt).toBe(now);
    expect(task.endedAt).toBe(now);
    expect(task?.cleanupAfter).toBe(now + 7 * 24 * 60 * 60_000);

    const delays = timeoutSpy.mock.calls
      .map(([, delay]) => delay)
      .filter((delay): delay is number => typeof delay === "number");
    const positiveDelays = delays.filter((delay) => delay > 0);
    expect(positiveDelays.length).toBeGreaterThan(0);

    timeoutSpy.mockRestore();
  });

  it("keeps scheduler progress when task ledger creation fails", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T12:00:00.000Z");
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();
    const ledgerError = new Error("disk full");

    await writeCronStoreSnapshot({
      storePath,
      jobs: [createDueMainJob({ now, wakeMode: "next-heartbeat" })],
    });

    const createTaskRecordSpy = vi
      .spyOn(detachedTaskRuntime, "createRunningTaskRun")
      .mockImplementation(() => {
        throw ledgerError;
      });

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
    });

    await onTimer(state);

    expect(logger.warn).toHaveBeenCalledWith(
      { jobId: "main-heartbeat-job", error: ledgerError },
      "cron: failed to create task ledger record",
    );
    expect(enqueueSystemEvent).toHaveBeenCalledWith("heartbeat seam tick", {
      agentId: undefined,
      sessionKey: "agent:main:main",
      contextKey: "cron:main-heartbeat-job",
    });

    createTaskRecordSpy.mockRestore();
  });

  it("reloads externally edited split-store schedules without firing stale slots", async () => {
    const { storePath } = await makeStorePath();
    const now = Date.parse("2026-03-23T06:00:00.000Z");
    const staleNextRunAtMs = now;
    const enqueueSystemEvent = vi.fn();
    const requestHeartbeat = vi.fn();

    await saveCronStore(storePath, {
      version: 1,
      jobs: [
        {
          id: "externally-edited-cron",
          name: "externally edited cron",
          enabled: true,
          createdAtMs: now - 60_000,
          updatedAtMs: now - 60_000,
          schedule: { kind: "cron", expr: "0 6 * * *", tz: "UTC" },
          sessionTarget: "main",
          wakeMode: "now",
          payload: { kind: "systemEvent", text: "stale schedule should not run" },
          state: { nextRunAtMs: staleNextRunAtMs },
        },
      ],
    });

    const config = JSON.parse(await fs.readFile(storePath, "utf8")) as {
      jobs: Array<Record<string, unknown>>;
    };
    config.jobs[0].schedule = { kind: "cron", expr: "0 7 * * *", tz: "UTC" };
    await fs.writeFile(storePath, JSON.stringify(config, null, 2), "utf8");

    const state = createCronServiceState({
      storePath,
      cronEnabled: true,
      log: logger,
      nowMs: () => now,
      enqueueSystemEvent,
      requestHeartbeat,
      runIsolatedAgentJob: vi.fn(async () => ({ status: "ok" as const })),
    });

    await onTimer(state);

    expect(enqueueSystemEvent).not.toHaveBeenCalled();
    expect(requestHeartbeat).not.toHaveBeenCalled();

    const persisted = await loadCronStore(storePath);
    const job = persisted.jobs[0];
    expect(job?.schedule).toEqual({ kind: "cron", expr: "0 7 * * *", tz: "UTC" });
    expect(job?.state.lastStatus).toBeUndefined();
    expect(job?.state.nextRunAtMs).toBe(Date.parse("2026-03-23T07:00:00.000Z"));
  });
});
