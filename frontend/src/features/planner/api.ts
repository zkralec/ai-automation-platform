import { apiRequest } from "@/lib/api/client";
import type { PlannerConfigOut, PlannerTemplateOut } from "@/lib/api/generated/openapi";

export type PlannerConfigPatchInput = Partial<
  Pick<
    PlannerConfigOut,
    | "enabled"
    | "execution_enabled"
    | "require_approval"
    | "approved"
    | "interval_sec"
    | "max_create_per_cycle"
    | "max_execute_per_cycle"
    | "max_pending_tasks"
  >
>;

export type PlannerTemplateCreateInput = {
  name: string;
  task_type: string;
  payload_json: string;
  model?: string | null;
  max_attempts?: number;
  min_interval_seconds?: number;
  enabled?: boolean;
  priority?: number;
  metadata_json?: Record<string, unknown> | null;
  id?: string;
};

export function getPlannerConfig(): Promise<PlannerConfigOut> {
  return apiRequest<PlannerConfigOut>("/planner/config");
}

export function patchPlannerConfig(input: PlannerConfigPatchInput): Promise<PlannerConfigOut> {
  return apiRequest<PlannerConfigOut>("/planner/config", { method: "PATCH", body: input });
}

export function resetPlannerConfig(): Promise<PlannerConfigOut> {
  return apiRequest<PlannerConfigOut>("/planner/config/reset", { method: "POST" });
}

export function listPlannerTemplates(limit = 100): Promise<PlannerTemplateOut[]> {
  return apiRequest<PlannerTemplateOut[]>(`/planner/templates?limit=${encodeURIComponent(limit)}`, {
    timeoutMs: 30_000
  });
}

export function createPlannerTemplate(input: PlannerTemplateCreateInput): Promise<PlannerTemplateOut> {
  return apiRequest<PlannerTemplateOut>("/planner/templates", { method: "POST", body: input });
}

export function patchPlannerTemplate(templateId: string, patch: Partial<PlannerTemplateOut>): Promise<PlannerTemplateOut> {
  return apiRequest<PlannerTemplateOut>(`/planner/templates/${encodeURIComponent(templateId)}`, {
    method: "PATCH",
    body: patch
  });
}

export function deletePlannerTemplate(templateId: string): Promise<{ deleted: boolean; template_id: string }> {
  return apiRequest<{ deleted: boolean; template_id: string }>(`/planner/templates/${encodeURIComponent(templateId)}`, {
    method: "DELETE"
  });
}

export function runPlannerOnce(): Promise<Record<string, unknown>> {
  return apiRequest<Record<string, unknown>>("/planner/run-once", { method: "POST" });
}

export function saveRtx5090Preset(input: {
  interval_seconds: number;
  gpu_max_price?: number | null;
  pc_max_price?: number | null;
  enabled?: boolean;
}): Promise<PlannerTemplateOut> {
  return apiRequest<PlannerTemplateOut>("/planner/templates/presets/rtx5090", { method: "POST", body: input });
}

export function saveJobsPreset(input: {
  interval_seconds: number;
  desired_title?: string | null;
  desired_titles?: string[] | null;
  keywords?: string[] | null;
  excluded_keywords?: string[] | null;
  preferred_locations?: string[] | null;
  remote_preference?: string[] | null;
  minimum_salary?: number | null;
  experience_level?: string | null;
  enabled_sources?: string[] | null;
  result_limit_per_source?: number | null;
  shortlist_count?: number | null;
  freshness_preference?: string | null;
  desired_salary_min?: number | null;
  desired_salary_max?: number | null;
  experience_levels?: string[] | null;
  clearance_required?: boolean | null;
  location?: string | null;
  boards?: string[] | null;
  enabled?: boolean;
}): Promise<PlannerTemplateOut> {
  return apiRequest<PlannerTemplateOut>("/planner/templates/presets/jobs-digest", { method: "POST", body: input });
}
