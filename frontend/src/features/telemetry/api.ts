import { ApiError, apiRequest, apiRequestText } from "@/lib/api/client";
import type {
  AgentHeartbeatOut,
  AiUsageSummaryOut,
  EventOut,
  HealthOut,
  PlannerStatusOut,
  ReadyOut,
  StatsToday,
  SystemMetricsOut
} from "@/lib/api/generated/openapi";

export function getStatsToday(): Promise<StatsToday> {
  return apiRequest<StatsToday>("/stats/today");
}

export function getEvents(limit = 100): Promise<EventOut[]> {
  return apiRequest<EventOut[]>(`/telemetry/events?limit=${encodeURIComponent(limit)}`);
}

export function getAiUsageSummary(): Promise<AiUsageSummaryOut> {
  return apiRequest<AiUsageSummaryOut>("/telemetry/ai-usage/summary");
}

export function getSystemLatest(): Promise<SystemMetricsOut | null> {
  return apiRequest<SystemMetricsOut | null>("/telemetry/system-metrics/latest");
}

export function getSystemRows(limit = 50): Promise<SystemMetricsOut[]> {
  return apiRequest<SystemMetricsOut[]>(`/telemetry/system-metrics?limit=${encodeURIComponent(limit)}`);
}

export function getPlannerStatus(eventLimit = 300): Promise<PlannerStatusOut> {
  return apiRequest<PlannerStatusOut>(`/telemetry/planner/status?event_limit=${encodeURIComponent(eventLimit)}`);
}

export function getPromMetrics(): Promise<string> {
  return apiRequestText("/metrics");
}

export function getHealthStatus(): Promise<HealthOut> {
  return apiRequest<HealthOut>("/health");
}

export async function getReadyStatus(): Promise<ReadyOut> {
  try {
    return await apiRequest<ReadyOut>("/ready");
  } catch (error) {
    if (error instanceof ApiError && typeof error.body === "object" && error.body !== null) {
      const status = String((error.body as { status?: string }).status || "not_ready");
      const bodyError = (error.body as { error?: string }).error;
      return { status, error: bodyError || error.message };
    }
    return { status: "not_ready", error: String(error) };
  }
}

export function getHeartbeats(limit = 100): Promise<AgentHeartbeatOut[]> {
  return apiRequest<AgentHeartbeatOut[]>(`/telemetry/heartbeats?limit=${encodeURIComponent(limit)}`);
}

export function getStaleHeartbeats(staleAfterSeconds?: number, limit = 100): Promise<AgentHeartbeatOut[]> {
  const query = new URLSearchParams();
  query.set("limit", String(limit));
  if (typeof staleAfterSeconds === "number" && Number.isFinite(staleAfterSeconds)) {
    query.set("stale_after_seconds", String(Math.max(1, Math.trunc(staleAfterSeconds))));
  }
  return apiRequest<AgentHeartbeatOut[]>(`/telemetry/heartbeats/stale?${query.toString()}`);
}

export type HeartbeatSummaryOut = {
  captured_at: string;
  stale_cutoff_at?: string;
  stale_after_seconds: number;
  tracked_agent_names: string[];
  tracked_agents_total: number;
  active_tracked_agents: number;
  stale_current_agents: number;
  historical_dead_agents: number;
  active_tracked_rows: AgentHeartbeatOut[];
  stale_current_rows: AgentHeartbeatOut[];
  historical_dead_rows: AgentHeartbeatOut[];
};

export function getHeartbeatSummary(staleAfterSeconds?: number, limit = 200): Promise<HeartbeatSummaryOut> {
  const query = new URLSearchParams();
  query.set("limit", String(limit));
  if (typeof staleAfterSeconds === "number" && Number.isFinite(staleAfterSeconds)) {
    query.set("stale_after_seconds", String(Math.max(1, Math.trunc(staleAfterSeconds))));
  }
  return apiRequest<HeartbeatSummaryOut>(`/telemetry/heartbeats/summary?${query.toString()}`);
}
