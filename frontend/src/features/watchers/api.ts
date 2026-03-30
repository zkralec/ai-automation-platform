import { apiRequest } from "@/lib/api/client";

export type WatcherRunSummary = {
  task_id: string;
  task_status: string;
  task_updated_at: string;
  task_created_at: string;
  error?: string | null;
  run_id?: string | null;
  run_attempt?: number | null;
  run_status?: string | null;
  run_started_at?: string | null;
  run_ended_at?: string | null;
  run_wall_time_ms?: number | null;
};

export type WatcherOutcomeSummary = {
  status?: string | null;
  message?: string | null;
  artifact_type?: string | null;
  created_at?: string | null;
};

export type Watcher = {
  id: string;
  name: string;
  task_type: string;
  payload_json: string;
  model?: string | null;
  max_attempts: number;
  interval_seconds: number;
  min_interval_seconds: number;
  enabled: boolean;
  priority: number;
  notification_behavior?: Record<string, unknown> | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  last_run_summary?: WatcherRunSummary | null;
  last_outcome_summary?: WatcherOutcomeSummary | null;
};

export type WatcherCreateInput = {
  id?: string;
  name: string;
  task_type: string;
  payload_json: string;
  model?: string | null;
  max_attempts?: number;
  interval_seconds?: number;
  min_interval_seconds?: number;
  enabled?: boolean;
  priority?: number;
  notification_behavior?: Record<string, unknown> | null;
  metadata?: Record<string, unknown> | null;
};

export type WatcherPatchInput = Partial<{
  name: string;
  task_type: string;
  payload_json: string;
  model: string | null;
  max_attempts: number;
  interval_seconds: number;
  min_interval_seconds: number;
  enabled: boolean;
  priority: number;
  notification_behavior: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
}>;

export function listWatchers(limit = 100, enabledOnly = false): Promise<Watcher[]> {
  return apiRequest<Watcher[]>(`/watchers?limit=${encodeURIComponent(limit)}&enabled_only=${enabledOnly ? "true" : "false"}`);
}

export function getWatcher(watcherId: string): Promise<Watcher> {
  return apiRequest<Watcher>(`/watchers/${encodeURIComponent(watcherId)}`);
}

export function createWatcher(input: WatcherCreateInput): Promise<Watcher> {
  return apiRequest<Watcher>("/watchers", { method: "POST", body: input });
}

export function patchWatcher(watcherId: string, patch: WatcherPatchInput): Promise<Watcher> {
  return apiRequest<Watcher>(`/watchers/${encodeURIComponent(watcherId)}`, {
    method: "PATCH",
    body: patch
  });
}

export function deleteWatcher(watcherId: string): Promise<{ deleted: boolean; watcher_id: string }> {
  return apiRequest<{ deleted: boolean; watcher_id: string }>(`/watchers/${encodeURIComponent(watcherId)}`, {
    method: "DELETE"
  });
}
