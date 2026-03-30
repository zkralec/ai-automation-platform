import { MemoryRouter } from "react-router-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Watcher } from "@/features/watchers/api";
import { WorkflowsPage } from "@/pages/workflows-page";

const mutateCreateTask = vi.fn();
const mutateSaveRtx = vi.fn();
const mutateSaveJobs = vi.fn();
let watchersFixture: Watcher[] = [];

vi.mock("@/features/tasks/queries", () => ({
  useCreateTaskMutation: () => ({ mutate: mutateCreateTask, isPending: false, error: null }),
  useTasks: () => ({ data: [], isLoading: false, error: null, refetch: vi.fn() })
}));

vi.mock("@/features/planner/queries", () => ({
  usePlannerConfig: () => ({
    data: { enabled: true, execution_enabled: false, require_approval: true, approved: false, interval_sec: 300, max_create_per_cycle: 1, max_execute_per_cycle: 2 },
    error: null,
    refetch: vi.fn()
  }),
  usePlannerStatus: () => ({ data: { recent_summary_24h: { cycles: 0, executed_actions: 0 } }, error: null, refetch: vi.fn() }),
  usePatchPlannerConfigMutation: () => ({ mutate: vi.fn(), isPending: false, error: null }),
  useResetPlannerConfigMutation: () => ({ mutate: vi.fn(), isPending: false, error: null }),
  useRunPlannerOnceMutation: () => ({ mutate: vi.fn(), isPending: false, error: null }),
  useSaveRtxPresetMutation: () => ({ mutate: mutateSaveRtx, isPending: false, error: null }),
  useSaveJobsPresetMutation: () => ({ mutate: mutateSaveJobs, isPending: false, error: null })
}));

vi.mock("@/features/watchers/queries", () => ({
  useWatchers: () => ({ data: watchersFixture, isLoading: false, error: null, refetch: vi.fn() }),
  useCreateWatcherMutation: () => ({ mutate: vi.fn(), isPending: false, error: null }),
  useDeleteWatcherMutation: () => ({ mutate: vi.fn(), isPending: false, error: null }),
  usePatchWatcherMutation: () => ({ mutate: vi.fn(), isPending: false, error: null })
}));

describe("WorkflowsPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    watchersFixture = [];
  });

  it("wires quick run actions to task creation", () => {
    render(
      <MemoryRouter>
        <WorkflowsPage />
      </MemoryRouter>
    );

    const runButtons = screen.getAllByRole("button", { name: /^Run$/i });
    fireEvent.click(runButtons[0]);
    expect(mutateCreateTask).toHaveBeenCalled();
  });

  it("uses notify_v1 payload shape required by schema", () => {
    render(
      <MemoryRouter>
        <WorkflowsPage />
      </MemoryRouter>
    );

    const runButtons = screen.getAllByRole("button", { name: /^Run$/i });
    fireEvent.click(runButtons[2]);

    expect(mutateCreateTask).toHaveBeenCalled();
    const payloadJson = mutateCreateTask.mock.calls[0]?.[0]?.payload_json;
    const payload = JSON.parse(String(payloadJson)) as Record<string, unknown>;
    expect(Array.isArray(payload.channels)).toBe(true);
    expect(payload.channels).toEqual(["discord"]);
    expect(payload.source_task_type).toBe("ops_report_v1");
    expect(payload.disable_dedupe).toBe(true);
    expect(payload).not.toHaveProperty("channel");
  });

  it("wires preset actions", () => {
    render(
      <MemoryRouter>
        <WorkflowsPage />
      </MemoryRouter>
    );

    const saveIntervalButtons = screen.getAllByRole("button", { name: /Save Interval/i });
    fireEvent.click(saveIntervalButtons[1]);
    fireEvent.click(saveIntervalButtons[2]);

    expect(mutateSaveRtx).toHaveBeenCalled();
    expect(mutateSaveJobs).toHaveBeenCalled();
  });

  it("shows derived operator summary for workflow details without raw payload internals", () => {
    watchersFixture = [
      {
        id: "watcher-daily-ops-notify",
        name: "Daily Ops Notifications",
        task_type: "notify_v1",
        payload_json: JSON.stringify({
          channels: ["discord"],
          severity: "urgent",
          dedupe_ttl_seconds: 900,
          message: "Ops alert"
        }),
        max_attempts: 3,
        interval_seconds: 600,
        min_interval_seconds: 600,
        enabled: true,
        priority: 30,
        notification_behavior: {
          mode: "digest",
          channel: "operator_default"
        },
        metadata: {},
        created_at: "2026-03-11T10:00:00Z",
        updated_at: "2026-03-11T10:10:00Z",
        last_run_summary: {
          task_id: "task-1",
          task_status: "failed_permanent",
          task_updated_at: "2026-03-11T10:09:00Z",
          task_created_at: "2026-03-11T10:08:00Z",
          error: "Delivery failed"
        },
        last_outcome_summary: {
          status: "error",
          message: "Discord delivery refused",
          artifact_type: "notification",
          created_at: "2026-03-11T10:09:00Z"
        }
      }
    ];

    render(
      <MemoryRouter>
        <WorkflowsPage />
      </MemoryRouter>
    );

    const detailButtons = screen.getAllByRole("button", { name: /Details/i });
    fireEvent.click(detailButtons[2]);

    expect(screen.getByText("Workflow Detail")).toBeInTheDocument();
    expect(screen.getByText("Notification Behavior")).toBeInTheDocument();
    expect(screen.getAllByText(/channels: discord/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/severity: urgent/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/dedupe ttl: 900s/i).length).toBeGreaterThan(0);
    expect(screen.getByText("Last Result Summary")).toBeInTheDocument();
    expect(screen.getAllByText(/discord delivery refused/i).length).toBeGreaterThan(0);
    expect(screen.getByText("Next Likely Action")).toBeInTheDocument();
    expect(screen.queryByText(/"channels"/i)).not.toBeInTheDocument();
  });

  it("saves structured jobs watcher configuration without requiring raw JSON edits", () => {
    watchersFixture = [
      {
        id: "preset-jobs-digest-scan",
        name: "Jobs Pipeline v2",
        task_type: "jobs_collect_v1",
        payload_json: JSON.stringify({
          request: {
            titles: ["ML Engineer"],
            keywords: ["python"],
            locations: ["Remote"],
            enabled_sources: ["linkedin", "indeed"],
            result_limit_per_source: 20,
            shortlist_max_items: 5,
            shortlist_freshness_preference: "off"
          }
        }),
        max_attempts: 3,
        interval_seconds: 300,
        min_interval_seconds: 300,
        enabled: true,
        priority: 20,
        notification_behavior: null,
        metadata: {},
        created_at: "2026-03-11T10:00:00Z",
        updated_at: "2026-03-11T10:10:00Z",
        last_run_summary: null,
        last_outcome_summary: null
      }
    ];

    render(
      <MemoryRouter>
        <WorkflowsPage />
      </MemoryRouter>
    );

    const detailButtons = screen.getAllByRole("button", { name: /^Details$/i });
    fireEvent.click(detailButtons[1]);

    fireEvent.change(screen.getByLabelText("Desired Titles (Collection + Ranking)"), { target: { value: "ML Engineer, Data Engineer" } });
    fireEvent.change(screen.getByLabelText("Preferred Locations (Collection + Ranking)"), { target: { value: "Remote\nAustin, TX" } });
    fireEvent.change(screen.getByLabelText("Result Limit Per Source (Collection)"), { target: { value: "30" } });
    fireEvent.change(screen.getByLabelText("Top-N Shortlist Count (Digest Size)"), { target: { value: "7" } });
    fireEvent.click(screen.getByRole("button", { name: /Save Jobs Configuration/i }));

    expect(mutateSaveJobs).toHaveBeenCalled();
    const payload = mutateSaveJobs.mock.calls[0]?.[0] as Record<string, unknown>;
    expect(payload.interval_seconds).toBe(300);
    expect(payload.desired_titles).toEqual(["ML Engineer", "Data Engineer"]);
    expect(payload.preferred_locations).toEqual(["Remote", "Austin, TX"]);
    expect(payload.result_limit_per_source).toBe(30);
    expect(payload.shortlist_count).toBe(7);
    expect(payload.freshness_preference).toBe("off");
  });
});
