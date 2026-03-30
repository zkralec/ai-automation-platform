import { MemoryRouter } from "react-router-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { RunsPage } from "@/pages/runs-page";

let tasksFixture: Array<Record<string, unknown>> = [];
let runsFixture: Array<Record<string, unknown>> = [];
let taskFixture: Record<string, unknown> | null = null;
let taskRunsFixture: Array<Record<string, unknown>> = [];
let taskResultFixture: Record<string, unknown> | null = null;

vi.mock("@/features/tasks/queries", () => ({
  useTasks: () => ({
    data: tasksFixture,
    isLoading: false,
    error: null,
    refetch: vi.fn()
  }),
  useRuns: () => ({
    data: runsFixture,
    isLoading: false,
    error: null,
    refetch: vi.fn()
  }),
  useTask: () => ({
    data: taskFixture,
    error: null,
    refetch: vi.fn()
  }),
  useTaskRuns: () => ({
    data: taskRunsFixture,
    isLoading: false,
    error: null,
    refetch: vi.fn()
  }),
  useTaskResult: () => ({
    data: taskResultFixture,
    isLoading: false,
    error: null,
    refetch: vi.fn()
  })
}));

describe("RunsPage", () => {
  beforeEach(() => {
    tasksFixture = [
      {
        id: "task-1",
        created_at: "2026-03-11T00:00:00Z",
        task_type: "jobs_digest_v1",
        status: "failed",
        model: "gpt-5-mini",
        cost_usd: 0.01,
        updated_at: "2026-03-11T00:00:00Z",
        payload_json: "{\"q\":\"jobs\"}",
        max_attempts: 3
      }
    ];
    runsFixture = [{ id: "run-1", task_id: "task-1", attempt: 1, status: "failed", created_at: "2026-03-11T00:00:00Z" }];
    taskFixture = {
      id: "task-1",
      created_at: "2026-03-11T00:00:00Z",
      updated_at: "2026-03-11T00:00:00Z",
      status: "failed",
      task_type: "jobs_digest_v1",
      payload_json: "{\"q\":\"jobs\"}",
      max_attempts: 3
    };
    taskRunsFixture = [{ id: "run-1", task_id: "task-1", created_at: "2026-03-11T00:00:00Z", attempt: 1, status: "failed", wall_time_ms: 1200, cost_usd: 0.01 }];
    taskResultFixture = {
      task_id: "task-1",
      artifact_type: "result.json",
      created_at: "2026-03-11T00:00:00Z",
      content_json: { summary: "result", jobs: [{ title: "SE", company: "ACME" }] }
    };
  });

  it("opens detail surface after selecting a run row", () => {
    render(
      <MemoryRouter>
        <RunsPage />
      </MemoryRouter>
    );

    expect(screen.queryByText("Result Preview")).not.toBeInTheDocument();
    fireEvent.click(screen.getByText("jobs_digest_v1"));
    expect(screen.getAllByText("Result Preview").length).toBeGreaterThan(0);
  });

  it("shows jobs collection observability preview for jobs pipeline stages", () => {
    tasksFixture = [
      {
        id: "task-collect-1",
        created_at: "2026-03-11T00:00:00Z",
        task_type: "jobs_collect_v1",
        status: "success",
        model: "gpt-5-mini",
        cost_usd: 0.01,
        updated_at: "2026-03-11T00:00:00Z",
        payload_json: "{\"q\":\"jobs\"}",
        max_attempts: 3
      }
    ];
    runsFixture = [{ id: "run-1", task_id: "task-collect-1", attempt: 1, status: "success", created_at: "2026-03-11T00:00:00Z" }];
    taskFixture = {
      id: "task-collect-1",
      created_at: "2026-03-11T00:00:00Z",
      updated_at: "2026-03-11T00:00:00Z",
      status: "success",
      task_type: "jobs_collect_v1",
      payload_json: "{\"q\":\"jobs\"}",
      max_attempts: 3
    };
    taskRunsFixture = [{ id: "run-1", task_id: "task-collect-1", created_at: "2026-03-11T00:00:00Z", attempt: 1, status: "success", wall_time_ms: 1200, cost_usd: 0.01 }];
    taskResultFixture = {
      task_id: "task-collect-1",
      artifact_type: "result.json",
      created_at: "2026-03-11T00:00:00Z",
      content_json: {
        request: { search_mode: "broad_discovery" },
        collection_observability: {
          waterfall: {
            raw_jobs_discovered: 220,
            kept_after_basic_filter: 180,
            jobs_dropped: 40,
            deduped_in_collection: 12,
            final_raw_jobs: 168
          },
          operator_questions: {
            searched_enough: "LinkedIn + Indeed active. LinkedIn contributed 120 raw jobs; Indeed contributed 40 raw jobs. 8 queries executed.",
            which_source_is_weak: "Indeed under target after 3 pages attempted.",
            why_raw_count_collapsed: "40 dropped in basic filtering and 12 deduped before returning 168 raw jobs.",
            are_we_missing_metadata: "Weakest metadata source: Indeed."
          },
          run_preview: {
            messages: [
              "LinkedIn + Indeed active",
              "LinkedIn contributed 120 raw jobs",
              "Indeed contributed 40 raw jobs",
              "Indeed is under target for this run"
            ]
          },
          by_source: {
            linkedin: {
              source_label: "LinkedIn",
              status: "success",
              raw_jobs_discovered: 120,
              kept_after_basic_filter: 100,
              jobs_dropped: 20,
              deduped_in_collection: 5,
              jobs_kept: 100,
              pages_attempted: 5,
              under_target: false,
              suspected_blocking: false,
              weakness_summary: "post date 18%"
            },
            indeed: {
              source_label: "Indeed",
              status: "under_target",
              raw_jobs_discovered: 40,
              kept_after_basic_filter: 0,
              jobs_dropped: 12,
              deduped_in_collection: 4,
              jobs_kept: 0,
              pages_attempted: 3,
              under_target: true,
              suspected_blocking: false,
              weakness_summary: "company 46%, post date 71%"
            }
          }
        }
      }
    };

    render(
      <MemoryRouter>
        <RunsPage />
      </MemoryRouter>
    );

    fireEvent.click(screen.getByText("jobs_collect_v1"));
    expect(screen.getAllByText("Did We Search Enough?").length).toBeGreaterThan(0);
    expect(screen.getAllByText("LinkedIn + Indeed active. LinkedIn contributed 120 raw jobs; Indeed contributed 40 raw jobs. 8 queries executed.").length).toBeGreaterThan(0);
    expect(screen.getAllByText("LinkedIn contributed 120 raw jobs").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Indeed is under target for this run").length).toBeGreaterThan(0);
    expect(screen.getAllByText("LinkedIn").length).toBeGreaterThan(0);
    expect(screen.getAllByText("120 raw -> 100 kept").length).toBeGreaterThan(0);
    expect(screen.getAllByText("No usable jobs collected").length).toBeGreaterThan(0);
    expect(screen.getAllByText("under target").length).toBeGreaterThan(0);
    expect(screen.getAllByText(/company 46%, post date 71%/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/broad discovery/i).length).toBeGreaterThan(0);
  });

  it("supports task_id deep links and shows shortlist resurfacing summary", () => {
    tasksFixture = [
      {
        id: "task-shortlist-1",
        created_at: "2026-03-12T00:00:00Z",
        task_type: "jobs_shortlist_v1",
        status: "success",
        model: "gpt-5-mini",
        cost_usd: 0.03,
        updated_at: "2026-03-12T00:05:00Z",
        payload_json: "{\"q\":\"jobs\"}",
        max_attempts: 3
      }
    ];
    runsFixture = [{ id: "run-shortlist-1", task_id: "task-shortlist-1", attempt: 1, status: "success", created_at: "2026-03-12T00:00:00Z" }];
    taskFixture = tasksFixture[0];
    taskRunsFixture = [{ id: "run-shortlist-1", task_id: "task-shortlist-1", created_at: "2026-03-12T00:00:00Z", attempt: 1, status: "success", wall_time_ms: 900, cost_usd: 0.03 }];
    taskResultFixture = {
      task_id: "task-shortlist-1",
      artifact_type: "result.json",
      created_at: "2026-03-12T00:05:00Z",
      content_json: {
        shortlist_count: 2,
        history_observability: {
          selected_newly_discovered_count: 1,
          selected_resurfaced_count: 1,
          selected_previously_shortlisted_count: 0,
          selected_previously_notified_count: 0,
          cooldown_suppressed_count: 3
        },
        shortlist: [
          {
            title: "Backend Engineer",
            company: "Acme",
            source: "linkedin",
            newly_discovered: true
          },
          {
            title: "Software Engineer II",
            company: "Orbit",
            source: "indeed",
            resurfaced_from_prior_runs: true
          }
        ]
      }
    };

    render(
      <MemoryRouter initialEntries={["/runs?task_id=task-shortlist-1"]}>
        <RunsPage />
      </MemoryRouter>
    );

    expect(screen.getAllByText("History / Repeat Behavior").length).toBeGreaterThan(0);
    expect(screen.getAllByText(/cooldown suppressed 3/i).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("link", { name: /Inspect Digest Artifact/i }).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/resurfaced/i).length).toBeGreaterThan(0);
  });

  it("shows digest preview notify decision and top job links", () => {
    tasksFixture = [
      {
        id: "task-digest-2",
        created_at: "2026-03-13T00:00:00Z",
        task_type: "jobs_digest_v2",
        status: "success",
        model: "gpt-5-mini",
        cost_usd: 0.04,
        updated_at: "2026-03-13T00:05:00Z",
        payload_json: "{\"q\":\"jobs\"}",
        max_attempts: 3
      }
    ];
    runsFixture = [{ id: "run-digest-2", task_id: "task-digest-2", attempt: 1, status: "success", created_at: "2026-03-13T00:00:00Z" }];
    taskFixture = tasksFixture[0];
    taskRunsFixture = [{ id: "run-digest-2", task_id: "task-digest-2", created_at: "2026-03-13T00:00:00Z", attempt: 1, status: "success", wall_time_ms: 1100, cost_usd: 0.04 }];
    taskResultFixture = {
      task_id: "task-digest-2",
      artifact_type: "result.json",
      created_at: "2026-03-13T00:05:00Z",
      content_json: {
        search_mode: "precision_match",
        summary: "Strong backend fit with two clear options.",
        generation_mode: "llm_primary",
        pipeline_counts: { shortlisted_count: 2 },
        notify_decision: { should_notify: true, reason: "shortlist_non_empty" },
        model_usage: { attempts: 2, fallback_used: false, strict_failure: false },
        digest_jobs: [
          {
            title: "Senior Backend Engineer",
            company: "Northstar",
            source: "linkedin",
            source_url: "https://example.com/jobs/1",
            posted_display: "Posted 2d ago"
          }
        ]
      }
    };

    render(
      <MemoryRouter>
        <RunsPage />
      </MemoryRouter>
    );

    fireEvent.click(screen.getByText("jobs_digest_v2"));
    expect(screen.getAllByText("Notify Decision").length).toBeGreaterThan(0);
    expect(screen.getAllByText("shortlist_non_empty").length).toBeGreaterThan(0);
    expect(screen.getAllByRole("link", { name: "https://example.com/jobs/1" }).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("link", { name: /Inspect Digest Artifact/i }).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/precision match/i).length).toBeGreaterThan(0);
  });
});
