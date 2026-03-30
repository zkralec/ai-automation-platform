import { describe, expect, it } from "vitest";
import { deriveCommandStripState } from "@/components/common/command-status-strip";

describe("deriveCommandStripState", () => {
  it("derives labels from health/ready/planner/api key", () => {
    const state = deriveCommandStripState({
      health: { status: "ok" },
      ready: { status: "ready" },
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
    expect(state.plannerModeLabel).toBe("execute");
    expect(state.plannerApprovalLabel).toBe("awaiting");
    expect(state.apiKeyLabel).toBe("present");
  });
});
