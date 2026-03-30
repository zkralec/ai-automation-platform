import { describe, expect, it } from "vitest";
import { deriveCommandStripState } from "@/components/common/command-status-strip";

describe("deriveCommandStripState", () => {
  it("derives labels from health/ready/planner/api key", () => {
    const state = deriveCommandStripState({
      health: { status: "ok" },
      ready: { status: "ready" },
      runtime: {
        captured_at: "2026-03-18T12:00:00Z",
        api_healthy: true,
        ready_status: "ready",
        ready_error: null,
        redis_reachable: true,
        queue_depth: 4,
        stale_after_seconds: 240,
        scheduler_heartbeat: {
          name: "scheduler",
          healthy: true,
          status: "alive",
          last_seen_at: "2026-03-18T11:59:58Z",
          age_seconds: 2,
          message: "Heartbeat is recent."
        },
        worker_heartbeat: {
          name: "worker",
          healthy: false,
          status: "stale",
          last_seen_at: "2026-03-18T11:55:00Z",
          age_seconds: 300,
          message: "Heartbeat age exceeds 240s."
        },
        last_scheduler_tick_at: "2026-03-18T11:59:57Z"
      },
      planner: {
        enabled: true,
        mode: "execute",
        execution_enabled: true,
        require_approval: true,
        approved: false,
        interval_sec: 300
      },
      apiKeyPresent: true
    });

    expect(state.healthLabel).toBe("ok");
    expect(state.readyLabel).toBe("ready");
    expect(state.apiLabel).toBe("healthy");
    expect(state.schedulerLabel).toBe("alive");
    expect(state.workerLabel).toBe("stale");
    expect(state.redisLabel).toBe("reachable");
    expect(state.queueDepthLabel).toBe("4");
    expect(state.plannerModeLabel).toBe("execute");
    expect(state.plannerApprovalLabel).toBe("awaiting");
    expect(state.apiKeyLabel).toBe("present");
    expect(state.lastSchedulerTickLabel).not.toBe("-");
  });
});
