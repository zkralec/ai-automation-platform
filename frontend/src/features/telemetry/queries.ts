import { useQuery } from "@tanstack/react-query";
import {
  getAiUsageSummary,
  getEvents,
  getHealthStatus,
  getHeartbeatSummary,
  getHeartbeats,
  getPlannerStatus,
  getPromMetrics,
  getReadyStatus,
  getStaleHeartbeats,
  getStatsToday,
  getSystemLatest,
  getSystemRows
} from "@/features/telemetry/api";

export const telemetryKeys = {
  stats: ["telemetry", "stats"] as const,
  events: (limit: number) => ["telemetry", "events", { limit }] as const,
  aiSummary: ["telemetry", "ai-summary"] as const,
  systemLatest: ["telemetry", "system-latest"] as const,
  systemRows: (limit: number) => ["telemetry", "system-rows", { limit }] as const,
  plannerStatus: ["telemetry", "planner-status"] as const,
  prom: ["telemetry", "prom"] as const,
  health: ["telemetry", "health"] as const,
  ready: ["telemetry", "ready"] as const,
  heartbeats: (limit: number) => ["telemetry", "heartbeats", { limit }] as const,
  heartbeatSummary: (staleAfterSeconds: number | "default", limit: number) =>
    ["telemetry", "heartbeats-summary", { staleAfterSeconds, limit }] as const,
  staleHeartbeats: (staleAfterSeconds: number | "default", limit: number) =>
    ["telemetry", "heartbeats-stale", { staleAfterSeconds, limit }] as const
};

export function useStatsToday() {
  return useQuery({ queryKey: telemetryKeys.stats, queryFn: getStatsToday, refetchInterval: 20_000 });
}

export function useTelemetryEvents(limit = 100) {
  return useQuery({ queryKey: telemetryKeys.events(limit), queryFn: () => getEvents(limit), refetchInterval: 12_000 });
}

export function useAiSummary() {
  return useQuery({ queryKey: telemetryKeys.aiSummary, queryFn: getAiUsageSummary, refetchInterval: 20_000 });
}

export function useSystemLatest() {
  return useQuery({ queryKey: telemetryKeys.systemLatest, queryFn: getSystemLatest, refetchInterval: 12_000 });
}

export function useSystemRows(limit = 50) {
  return useQuery({ queryKey: telemetryKeys.systemRows(limit), queryFn: () => getSystemRows(limit), refetchInterval: 20_000 });
}

export function usePlannerStatus() {
  return useQuery({ queryKey: telemetryKeys.plannerStatus, queryFn: () => getPlannerStatus(300), refetchInterval: 10_000 });
}

export function usePromMetrics() {
  return useQuery({ queryKey: telemetryKeys.prom, queryFn: getPromMetrics, refetchInterval: 20_000 });
}

export function useHealthStatus() {
  return useQuery({ queryKey: telemetryKeys.health, queryFn: getHealthStatus, refetchInterval: 10_000 });
}

export function useReadyStatus() {
  return useQuery({ queryKey: telemetryKeys.ready, queryFn: getReadyStatus, refetchInterval: 10_000 });
}

export function useHeartbeats(limit = 100) {
  return useQuery({
    queryKey: telemetryKeys.heartbeats(limit),
    queryFn: () => getHeartbeats(limit),
    refetchInterval: 12_000
  });
}

export function useHeartbeatSummary(staleAfterSeconds?: number, limit = 200) {
  const queryStaleAfter = typeof staleAfterSeconds === "number" ? staleAfterSeconds : "default";
  return useQuery({
    queryKey: telemetryKeys.heartbeatSummary(queryStaleAfter, limit),
    queryFn: () => getHeartbeatSummary(staleAfterSeconds, limit),
    refetchInterval: 12_000
  });
}

export function useStaleHeartbeats(staleAfterSeconds?: number, limit = 100) {
  const queryStaleAfter = typeof staleAfterSeconds === "number" ? staleAfterSeconds : "default";
  return useQuery({
    queryKey: telemetryKeys.staleHeartbeats(queryStaleAfter, limit),
    queryFn: () => getStaleHeartbeats(staleAfterSeconds, limit),
    refetchInterval: 12_000
  });
}

export function parsePromMetrics(raw: string): Record<string, number> {
  const out: Record<string, number> = {};
  raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
    .forEach((line) => {
      const [metric, value] = line.split(/\s+/);
      const parsed = Number(value);
      if (metric && Number.isFinite(parsed)) out[metric] = parsed;
    });
  return out;
}
