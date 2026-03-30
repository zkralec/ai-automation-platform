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
            search_mode: "broad_discovery",
            titles: ["ML Engineer"],
            keywords: ["python"],
            locations: ["Remote"],
            enabled_sources: ["linkedin", "indeed"],
            result_limit_per_source: 120,
            minimum_raw_jobs_total: 120,
            minimum_unique_jobs_total: 80,
            minimum_jobs_per_source: 25,
            stop_when_minimum_reached: true,
            collection_time_cap_seconds: 120,
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
        workflow_summary: {
          kind: "jobs_watcher",
          search_mode: "broad_discovery",
          enabled_sources: ["linkedin", "indeed"],
          active_sources_label: "LinkedIn + Indeed active",
          source_contribution_summary: [
            "LinkedIn contributed 220 raw jobs",
            "Indeed contributed 200 raw jobs"
          ],
          query_count_used: 12,
          counts: {
            raw_jobs_found: 420,
            jobs_after_filtering: 310,
            jobs_after_dedupe: 180,
            shortlisted_count: 6,
            minimum_reached: true
          },
          notify: {
            status: "sent",
            reason: "shortlist_non_empty"
          },
          digest_preview: {
            headline: "Solid senior backend batch with good source diversity.",
            top_jobs: [
              {
                title: "Senior Software Engineer",
                company: "Acme",
                source: "indeed",
                source_url: "https://example.com/jobs/123",
                posted: "Posted 2d ago",
                reason: "Strong backend alignment."
              }
            ],
            source_diversity: ["indeed", "linkedin"],
            why_top_jobs_won: ["Strong metadata completeness"]
          },
          collection_quality: {
            minimum_targets: {
              minimum_raw_jobs_total_requested: 120,
              minimum_unique_jobs_total_requested: 80,
              minimum_jobs_per_source_requested: 25,
              minimum_reached: true,
              reason_stopped: "minimum_reached"
            },
            operator_summary: {
              searched_enough: "LinkedIn + Indeed active. LinkedIn contributed 220 raw jobs; Indeed contributed 200 raw jobs. 12 queries executed.",
              which_source_is_weak: "Lowest raw contribution came from Indeed.",
              why_did_raw_count_collapse: "Basic filtering removed 110 jobs.",
              are_we_missing_metadata: "Weakest metadata source: Indeed."
            },
            by_source: [
              {
                source: "linkedin",
                source_label: "LinkedIn",
                status: "success",
                raw_jobs_found: 220,
                kept_after_basic_filter: 180,
                jobs_dropped: 40,
                pages_attempted: 6,
                under_target: false,
                suspected_blocking: false,
                missing_company_rate: 2,
                missing_posted_at_rate: 6,
                missing_source_url_rate: 1,
                missing_location_rate: 4,
                weakness_summary: "post date 6%, location 4%"
              },
              {
                source: "indeed",
                source_label: "Indeed",
                status: "under_target",
                raw_jobs_found: 200,
                kept_after_basic_filter: 130,
                jobs_dropped: 70,
                pages_attempted: 4,
                under_target: true,
                suspected_blocking: false,
                missing_company_rate: 4,
                missing_posted_at_rate: 18,
                missing_source_url_rate: 12,
                missing_location_rate: 7,
                weakness_summary: "post date 18%, link 12%"
              }
            ]
          }
        },
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

    expect(screen.getByText("Latest Digest Preview")).toBeInTheDocument();
    expect(screen.getByText(/Solid senior backend batch with good source diversity/i)).toBeInTheDocument();
    expect(screen.getAllByText(/LinkedIn \+ Indeed active/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/LinkedIn contributed 220 raw jobs/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Indeed contributed 200 raw jobs/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/broad discovery/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/120 raw \/ 80 unique \/ 25 per source/i).length).toBeGreaterThan(0);
    expect(screen.getByText("Target Reached")).toBeInTheDocument();
    expect(screen.getAllByText(/^Yes$/i).length).toBeGreaterThan(0);
    expect(screen.getByText("Actual Counts")).toBeInTheDocument();
    expect(screen.getByText(/420 raw \/ 180 unique/i)).toBeInTheDocument();
    expect(screen.getByText("Stop Reason")).toBeInTheDocument();
    expect(screen.getAllByText(/Minimum reached/i).length).toBeGreaterThan(0);
    expect(screen.getByText(/Max caps are advanced guardrails/i)).toBeInTheDocument();
    expect(screen.getByText(/Minimum target = how much the pipeline tries to collect before stopping/i)).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Desired titles"), { target: { value: "ML Engineer, Data Engineer" } });
    fireEvent.change(screen.getByLabelText("Search mode"), { target: { value: "precision_match" } });
    fireEvent.change(screen.getByLabelText("Preferred locations"), { target: { value: "Remote\nAustin, TX" } });
    fireEvent.change(screen.getByLabelText("Result limit per source"), { target: { value: "30" } });
    fireEvent.change(screen.getByLabelText("Minimum raw jobs"), { target: { value: "90" } });
    fireEvent.change(screen.getByLabelText("Minimum unique jobs"), { target: { value: "60" } });
    fireEvent.change(screen.getByLabelText("Minimum per source"), { target: { value: "20" } });
    fireEvent.change(screen.getByLabelText("Collection time cap (seconds)"), { target: { value: "150" } });
    fireEvent.change(screen.getByLabelText("Max queries per run"), { target: { value: "14" } });
    fireEvent.change(screen.getByLabelText("Shortlist size"), { target: { value: "7" } });
    fireEvent.change(screen.getByLabelText("Notification cooldown days"), { target: { value: "5" } });
    fireEvent.change(screen.getByLabelText("Shortlist repeat penalty"), { target: { value: "6.5" } });
    fireEvent.click(screen.getByRole("button", { name: /Save Jobs Configuration/i }));

    expect(mutateSaveJobs).toHaveBeenCalled();
    const payload = mutateSaveJobs.mock.calls[0]?.[0] as Record<string, unknown>;
    expect(payload.interval_seconds).toBe(300);
    expect(payload.search_mode).toBe("precision_match");
    expect(payload.desired_titles).toEqual(["ML Engineer", "Data Engineer"]);
    expect(payload.preferred_locations).toEqual(["Remote", "Austin, TX"]);
    expect(payload.result_limit_per_source).toBe(30);
    expect(payload.minimum_raw_jobs_total).toBe(90);
    expect(payload.minimum_unique_jobs_total).toBe(60);
    expect(payload.minimum_jobs_per_source).toBe(20);
    expect(payload.stop_when_minimum_reached).toBe(true);
    expect(payload.collection_time_cap_seconds).toBe(150);
    expect(payload.max_queries_per_run).toBe(14);
    expect(payload.shortlist_count).toBe(7);
    expect(payload.jobs_notification_cooldown_days).toBe(5);
    expect(payload.jobs_shortlist_repeat_penalty).toBe(6.5);
    expect(payload.resurface_seen_jobs).toBe(true);
    expect(payload.freshness_preference).toBe("off");
  });
});
