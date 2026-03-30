import { MemoryRouter } from "react-router-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { RunsPage } from "@/pages/runs-page";

vi.mock("@/features/tasks/queries", () => ({
  useTasks: () => ({
    data: [
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
    ],
    isLoading: false,
    error: null,
    refetch: vi.fn()
  }),
  useRuns: () => ({ data: [{ id: "run-1", task_id: "task-1", attempt: 1, status: "failed", created_at: "2026-03-11T00:00:00Z" }], isLoading: false, error: null, refetch: vi.fn() }),
  useTask: () => ({ data: { id: "task-1", created_at: "2026-03-11T00:00:00Z", updated_at: "2026-03-11T00:00:00Z", status: "failed", task_type: "jobs_digest_v1", payload_json: "{\"q\":\"jobs\"}", max_attempts: 3 }, error: null, refetch: vi.fn() }),
  useTaskRuns: () => ({ data: [{ id: "run-1", task_id: "task-1", created_at: "2026-03-11T00:00:00Z", attempt: 1, status: "failed", wall_time_ms: 1200, cost_usd: 0.01 }], isLoading: false, error: null, refetch: vi.fn() }),
  useTaskResult: () => ({ data: { task_id: "task-1", artifact_type: "result.json", created_at: "2026-03-11T00:00:00Z", content_json: { summary: "result", jobs: [{ title: "SE", company: "ACME" }] } }, isLoading: false, error: null, refetch: vi.fn() })
}));

describe("RunsPage", () => {
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
});
