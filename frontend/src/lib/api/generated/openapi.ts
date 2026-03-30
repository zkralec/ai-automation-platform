/*
 * This file is generated from FastAPI OpenAPI schema.
 * Refresh with: npm run generate:openapi
 */

export type TaskStatus = "queued" | "running" | "success" | "failed" | "failed_permanent" | "blocked_budget";
export type RunStatus = "queued" | "running" | "success" | "failed";

export interface FailureDiagnosticsOut {
  summary: string;
  category: string;
  source: string;
  stage: string;
  error_type?: string | null;
  error_message?: string | null;
  upstream_service?: string | null;
  retry_scheduled: boolean;
  retry_at?: string | null;
  is_browser_disconnect: boolean;
  is_task_execution_failure: boolean;
  is_queue_enqueue_failure: boolean;
  is_scheduler_runtime_failure: boolean;
}

export interface TaskOut {
  id: string;
  created_at: string;
  updated_at: string;
  status: TaskStatus;
  task_type: string;
  payload_json: string;
  idempotency_key?: string | null;
  model?: string | null;
  tokens_in?: number | null;
  tokens_out?: number | null;
  cost_usd?: number | null;
  error?: string | null;
  max_attempts: number;
  next_run_at?: string | null;
  max_cost_usd?: number | null;
  expected_tokens_in?: number | null;
  expected_tokens_out?: number | null;
  diagnostics?: FailureDiagnosticsOut | null;
}

export interface RunOut {
  id: string;
  task_id: string;
  attempt: number;
  status: RunStatus;
  started_at?: string | null;
  ended_at?: string | null;
  wall_time_ms?: number | null;
  model?: string | null;
  tokens_in?: number | null;
  tokens_out?: number | null;
  cost_usd?: number | null;
  error?: string | null;
  created_at: string;
  diagnostics?: FailureDiagnosticsOut | null;
}

export interface TaskResultOut {
  task_id: string;
  artifact_type: string;
  content_text?: string | null;
  content_json?: unknown;
  created_at: string;
}

export interface StatsToday {
  spend_usd: number;
  budget_usd: number;
  remaining_usd: number;
  buffer_usd: number;
  runs_count: number;
  success_count: number;
  failed_count: number;
}

export interface PlannerConfigOut {
  enabled: boolean;
  execution_enabled: boolean;
  require_approval: boolean;
  approved: boolean;
  interval_sec: number;
  max_create_per_cycle: number;
  max_execute_per_cycle: number;
  max_pending_tasks: number;
  failure_lookback_minutes: number;
  failure_alert_count_threshold: number;
  failure_alert_rate_threshold: number;
  stale_task_age_seconds: number;
  execute_task_cooldown_seconds: number;
  health_cpu_max_percent: number;
  health_memory_max_percent: number;
  health_disk_max_percent: number;
  cost_budget_usd?: number | null;
  token_budget?: number | null;
  create_task_cooldown_seconds: number;
  create_task_max_attempts: number;
  updated_at: string;
  updated_by?: string | null;
}

export interface PlannerTemplateOut {
  id: string;
  name: string;
  task_type: string;
  payload_json: string;
  model?: string | null;
  max_attempts: number;
  min_interval_seconds: number;
  enabled: boolean;
  priority: number;
  metadata_json?: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
}

export interface ResumeProfileOut {
  has_resume: boolean;
  resume_name?: string | null;
  resume_sha256?: string | null;
  resume_char_count: number;
  resume_preview?: string | null;
  metadata_json?: Record<string, unknown> | null;
  created_at?: string | null;
  updated_at?: string | null;
  resume_text?: string | null;
}

export interface SystemMetricsOut {
  id: string;
  cpu_percent?: number | null;
  memory_percent?: number | null;
  disk_percent?: number | null;
  load_avg_json?: number[] | null;
  created_at: string;
}

export interface EventOut {
  id?: string;
  event_type: string;
  source: string;
  level: string;
  message: string;
  metadata_json?: unknown;
  created_at: string;
}

export interface AiUsageSummaryOut {
  start: string;
  end: string;
  requests_total: number;
  succeeded_total: number;
  failed_total: number;
  tokens_in_total?: number | null;
  tokens_out_total?: number | null;
  total_tokens_sum?: number | null;
  cost_usd_total?: number | null;
  latency_ms_avg?: number | null;
}

export interface PlannerStatusOut {
  enabled: boolean;
  mode: string;
  execution_enabled: boolean;
  require_approval: boolean;
  approved: boolean;
  interval_sec: number;
  recent_summary_24h?: Record<string, unknown>;
  recent_events?: Array<Record<string, unknown>>;
}

export interface HealthOut {
  status: string;
  service: string;
  utc_now?: string;
}

export interface ReadyOut {
  status: string;
  error?: string;
}

export interface AgentHeartbeatOut {
  agent_name: string;
  last_seen_at: string;
  status: string;
  metadata_json?: unknown;
}

export interface RuntimeAgentStatusOut {
  name: string;
  healthy: boolean;
  status: string;
  last_seen_at?: string | null;
  age_seconds?: number | null;
  message?: string | null;
}

export interface RuntimeStatusOut {
  captured_at: string;
  api_healthy: boolean;
  ready_status: string;
  ready_error?: string | null;
  redis_reachable: boolean;
  queue_depth?: number | null;
  stale_after_seconds: number;
  scheduler_heartbeat: RuntimeAgentStatusOut;
  worker_heartbeat: RuntimeAgentStatusOut;
  last_scheduler_tick_at?: string | null;
}
