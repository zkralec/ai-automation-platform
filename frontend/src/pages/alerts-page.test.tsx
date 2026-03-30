import { MemoryRouter } from "react-router-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { EventOut, TaskOut } from "@/lib/api/generated/openapi";
import { AlertsPage } from "@/pages/alerts-page";

let telemetryEventsFixture: EventOut[] = [];
let tasksFixture: TaskOut[] = [];

vi.mock("@/features/telemetry/queries", () => ({
  useTelemetryEvents: () => ({
    data: telemetryEventsFixture,
    isLoading: false,
    error: null,
    refetch: vi.fn()
  })
}));

vi.mock("@/features/tasks/queries", () => ({
  useTasks: () => ({
    data: tasksFixture,
    isLoading: false,
    error: null,
    refetch: vi.fn()
  })
}));

describe("AlertsPage", () => {
  beforeEach(() => {
    telemetryEventsFixture = [
      {
        id: "evt-watchdog-1",
        event_type: "watchdog_agent_stale",
        source: "scheduler",
        level: "WARNING",
        message: "watchdog detected stale agent worker",
        metadata_json: { agent_name: "worker" },
        created_at: "2026-03-11T11:00:00Z"
      },
      {
        id: "evt-watchdog-2",
        event_type: "watchdog_agent_stale",
        source: "scheduler",
        level: "WARNING",
        message: "watchdog detected stale agent worker",
        metadata_json: { agent_name: "worker" },
        created_at: "2026-03-11T11:01:00Z"
      },
      {
        id: "evt-notify-failure",
        event_type: "task_failed",
        source: "worker",
        level: "ERROR",
        message: "Task failed permanently: notify_v1",
        metadata_json: { task_type: "notify_v1", channel: "discord" },
        created_at: "2026-03-11T11:02:00Z"
      }
    ];

    tasksFixture = [
      {
        id: "task-notify-1",
        created_at: "2026-03-11T10:55:00Z",
        updated_at: "2026-03-11T11:03:00Z",
        status: "failed_permanent",
        task_type: "notify_v1",
        payload_json: "{}",
        max_attempts: 3,
        error: "notify_v1 send failed for channel 'discord'",
        model: null
      },
      {
        id: "task-notify-2",
        created_at: "2026-03-11T10:56:00Z",
        updated_at: "2026-03-11T11:04:00Z",
        status: "failed_permanent",
        task_type: "notify_v1",
        payload_json: "{}",
        max_attempts: 3,
        error: "notify_v1 send failed for channel 'discord'",
        model: null
      }
    ];
  });

  it("shows direct actions and grouped watchdog/notify failures", () => {
    render(
      <MemoryRouter>
        <AlertsPage />
      </MemoryRouter>
    );

    expect(screen.getAllByRole("link", { name: /Open Matching Runs/i }).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("link", { name: /Open Workflow Config/i }).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("link", { name: /Open Observability/i }).length).toBeGreaterThan(0);
    expect(screen.getByText(/repeated watchdog\/heartbeat alerts grouped/i)).toBeInTheDocument();
    expect(screen.getByText(/repeated notification delivery failures grouped/i)).toBeInTheDocument();
  });

  it("supports quick focus filtering for system-only triage", () => {
    render(
      <MemoryRouter>
        <AlertsPage />
      </MemoryRouter>
    );

    fireEvent.click(screen.getByRole("button", { name: /System Alerts \(/i }));

    expect(screen.getAllByText(/^System Alerts$/i).length).toBeGreaterThan(0);
    expect(screen.queryByText(/^Action Needed$/i, { selector: "h2" })).not.toBeInTheDocument();
    expect(screen.queryByText(/^Workflow Alerts$/i, { selector: "h2" })).not.toBeInTheDocument();
  });
});
