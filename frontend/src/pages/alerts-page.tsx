import { useMemo, useState } from "react";
import { EmptyState } from "@/components/common/empty-state";
import { ErrorPanel } from "@/components/common/error-panel";
import { EventFeedList, type FeedAction, type FeedEvent } from "@/components/common/event-feed-list";
import { PageHeader } from "@/components/common/page-header";
import { SectionHeader } from "@/components/common/section-header";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { useTasks } from "@/features/tasks/queries";
import { useTelemetryEvents } from "@/features/telemetry/queries";
import type { EventOut, TaskOut } from "@/lib/api/generated/openapi";
import { errorMessage } from "@/lib/utils/errors";

type AlertCategory = "actionNeeded" | "workflow" | "system" | "informational";
type AlertSeverity = "error" | "warning" | "info";
type AlertFocus = "all" | "actionNeeded" | "workflow" | "system";
type AlertSignal = FeedEvent & {
  category: AlertCategory;
  severity: AlertSeverity;
  signature: string;
  priority: number;
  createdAtMs: number;
  family: string;
  taskTypeHint?: string | null;
  workflowRoute?: string | null;
};

const FAILING_TASK_STATUSES = new Set(["failed", "failed_permanent", "blocked_budget"]);
const TASK_TYPE_PATTERN = /([a-z0-9]+(?:_[a-z0-9]+)+_v\d+)/i;
const JOBS_WATCHER_ROUTE = "/workflows?watcher=preset-jobs-digest-scan";
const JOBS_SOURCE_COVERAGE_ROUTE = "/runs?task_type=jobs_collect_v1";
const JOBS_DIGEST_ROUTE = "/runs?task_type=jobs_digest_v2";

const PRIMARY_WORKFLOW_ROUTE_BY_TASK_TYPE: Record<string, string> = {
  deals_scan_v1: "/workflows?watcher=preset-rtx5090-deals-scan",
  jobs_collect_v1: "/workflows?watcher=preset-jobs-digest-scan",
  jobs_normalize_v1: "/workflows?watcher=preset-jobs-digest-scan",
  jobs_rank_v1: "/workflows?watcher=preset-jobs-digest-scan",
  jobs_shortlist_v1: "/workflows?watcher=preset-jobs-digest-scan",
  jobs_digest_v2: "/workflows?watcher=preset-jobs-digest-scan",
  jobs_digest_v1: "/workflows?watcher=preset-jobs-digest-scan",
  notify_v1: "/workflows?watcher=watcher-daily-ops-notify",
  ops_report_v1: "/workflows?watcher=watcher-daily-ops-notify"
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asText(value: unknown): string {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return "";
}

function parseMetadataRecord(metadata: unknown): Record<string, unknown> {
  if (isRecord(metadata)) return metadata;
  if (typeof metadata === "string") {
    try {
      const parsed = JSON.parse(metadata);
      if (isRecord(parsed)) return parsed;
    } catch {
      return {};
    }
  }
  return {};
}

function normalizeForSignature(input: string): string {
  return input
    .toLowerCase()
    .replace(/\b[0-9a-f]{8,}\b/g, "#")
    .replace(/\d+/g, "#")
    .replace(/\s+/g, " ")
    .trim();
}

function summarizeText(input: string, max = 180): string {
  const oneLine = input.replace(/\s+/g, " ").trim();
  if (!oneLine) return "No additional explanation available.";
  if (oneLine.length <= max) return oneLine;
  return `${oneLine.slice(0, max - 1)}…`;
}

function humanizeEventType(eventType: string): string {
  return eventType
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function severityWeight(severity: AlertSeverity): number {
  if (severity === "error") return 300;
  if (severity === "warning") return 200;
  return 100;
}

function categoryWeight(category: AlertCategory): number {
  if (category === "actionNeeded") return 100;
  if (category === "workflow") return 60;
  if (category === "system") return 50;
  return 20;
}

function parseTimestampMs(raw: string | undefined): number {
  if (!raw) return 0;
  const parsed = new Date(raw).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function inferTaskTypeHint(eventType: string, message: string, metadata: Record<string, unknown>): string | null {
  const metadataKeys = ["task_type", "source_task_type", "template_task_type", "workflow_task_type"];
  for (const key of metadataKeys) {
    const candidate = asText(metadata[key]).trim().toLowerCase();
    if (candidate && TASK_TYPE_PATTERN.test(candidate)) return candidate;
  }

  const text = `${eventType} ${message}`.toLowerCase();
  const matchedType = text.match(TASK_TYPE_PATTERN)?.[1];
  if (matchedType) return matchedType.toLowerCase();
  if (text.includes("notify") || text.includes("notification")) return "notify_v1";
  if (text.includes("deal") || text.includes("rtx") || text.includes("unicorn")) return "deals_scan_v1";
  if (text.includes("job") || text.includes("digest") || text.includes("resume")) return "jobs_collect_v1";
  return null;
}

function inferWorkflowRoute(taskTypeHint: string | null, eventType: string, message: string): string | null {
  if (taskTypeHint && PRIMARY_WORKFLOW_ROUTE_BY_TASK_TYPE[taskTypeHint]) {
    return PRIMARY_WORKFLOW_ROUTE_BY_TASK_TYPE[taskTypeHint];
  }
  const text = `${eventType} ${message}`.toLowerCase();
  if (text.includes("notify") || text.includes("notification")) return PRIMARY_WORKFLOW_ROUTE_BY_TASK_TYPE.notify_v1;
  if (text.includes("deals") || text.includes("rtx")) return PRIMARY_WORKFLOW_ROUTE_BY_TASK_TYPE.deals_scan_v1;
  if (text.includes("jobs") || text.includes("digest")) return PRIMARY_WORKFLOW_ROUTE_BY_TASK_TYPE.jobs_collect_v1;
  if (text.includes("workflow") || text.includes("template") || text.includes("planner")) return "/workflows";
  return null;
}

function isJobsTaskType(taskTypeHint: string | null | undefined): boolean {
  return Boolean(taskTypeHint && taskTypeHint.startsWith("jobs_"));
}

function extractTaskIdHint(metadata: Record<string, unknown>): string | null {
  const keys = ["task_id", "source_task_id", "parent_task_id", "latest_task_id"];
  for (const key of keys) {
    const value = asText(metadata[key]).trim();
    if (value) return value;
  }
  return null;
}

function extractJobsSourceHint(metadata: Record<string, unknown>): string {
  const keys = ["job_source", "source_name", "weak_source", "adapter", "adapter_name", "source"];
  for (const key of keys) {
    const value = asText(metadata[key]).trim();
    if (value) return value;
  }
  return "";
}

function isIntentionalJobsNotifySkip(message: string, metadata: Record<string, unknown>): boolean {
  const reason = [
    message,
    asText(metadata.reason),
    asText(metadata.notify_reason),
    asText(metadata.skip_reason),
    asText(metadata.notify_decision),
    asText(metadata.should_notify)
  ]
    .join(" ")
    .toLowerCase();

  return (
    reason.includes("notify skipped") ||
    reason.includes("skipped_empty_shortlist") ||
    reason.includes("empty shortlist") ||
    reason.includes("should_notify false") ||
    reason.includes("intentional skip")
  );
}

function inferAgentHint(message: string, metadata: Record<string, unknown>, source: string): string {
  const metadataAgent =
    asText(metadata.agent_name) ||
    asText(metadata.scheduler_name) ||
    asText(metadata.worker_name);
  if (metadataAgent.trim()) return normalizeForSignature(metadataAgent);

  const messageAgent = message.match(/\b(agent|worker|scheduler)\s*[:=]?\s*([a-z0-9_.-]+)/i)?.[2];
  if (messageAgent) return normalizeForSignature(messageAgent);
  return normalizeForSignature(source || "unknown");
}

function classifyFamily(eventType: string, message: string, taskTypeHint: string | null): string {
  const text = `${eventType} ${message}`.toLowerCase();
  const notifyFailureSignal =
    (taskTypeHint === "notify_v1" || text.includes("notify") || text.includes("notification")) &&
    (text.includes("fail") || text.includes("error") || text.includes("rejected") || text.includes("not_sent") || text.includes("delivery"));
  if (notifyFailureSignal) return "notify-failure";
  if (text.includes("watchdog")) return "watchdog";
  if (text.includes("stale") && (text.includes("agent") || text.includes("heartbeat"))) return "stale-agent";
  if (text.includes("stale")) return "stale";
  if (text.includes("planner") && text.includes("tick")) return "planner-tick";
  if (text.includes("planner") && text.includes("cycle")) return "planner-cycle";
  if (text.includes("task_failed") || text.includes("failed_permanent")) return "task-failure";
  if (text.includes("blocked_budget") || text.includes("budget")) return "budget";
  return "generic";
}

function inferSeverity(event: EventOut): AlertSeverity {
  const level = String(event.level || "info").toLowerCase();
  const text = `${event.event_type} ${event.message}`.toLowerCase();

  if (level === "error") return "error";
  if (text.includes("failed_permanent") || text.includes("blocked_budget") || text.includes("critical")) return "error";
  if (level === "warning") return "warning";
  if (text.includes("failed") || text.includes("stale") || text.includes("watchdog") || text.includes("not_ready")) return "warning";
  return "info";
}

function inferCategory(event: EventOut, severity: AlertSeverity): AlertCategory {
  const type = String(event.event_type || "").toLowerCase();
  const source = String(event.source || "").toLowerCase();
  const text = `${type} ${event.message}`.toLowerCase();

  const urgentSignals = ["failed_permanent", "blocked_budget", "task_failed", "auth_rejected", "unauthorized", "rate_limited"];
  if (urgentSignals.some((signal) => text.includes(signal))) return "actionNeeded";
  if (severity === "error") return "actionNeeded";

  const systemSignals = ["watchdog", "stale", "heartbeat", "cpu", "memory", "disk", "health", "not_ready"];
  if (systemSignals.some((signal) => text.includes(signal))) return "system";

  const workflowSignals = ["planner", "task_", "template", "workflow", "deals", "jobs", "resume", "digest", "notify"];
  if (workflowSignals.some((signal) => text.includes(signal)) || ["scheduler", "worker", "planner", "notify_v1"].includes(source)) {
    return "workflow";
  }

  return "informational";
}

function inferNextAction(eventType: string, message: string, family: string): string | undefined {
  const text = `${eventType} ${message}`.toLowerCase();
  if (text.includes("blocked_budget") || text.includes("budget")) {
    return "Open Workflows and adjust budget policy, then rerun blocked tasks from Runs.";
  }
  if (family === "notify-failure") {
    return "Open workflow config, verify notification channel and dedupe settings, then retry matching runs.";
  }
  if (text.includes("failed_permanent") || text.includes("task_failed") || text.includes("failed")) {
    return "Open matching runs and inspect attempts, payload, and result artifacts.";
  }
  if (text.includes("watchdog") || text.includes("stale") || text.includes("heartbeat")) {
    return "Open Observability and verify heartbeat health for scheduler and worker agents.";
  }
  if (text.includes("planner") || text.includes("approval")) {
    return "Open Workflows and verify planner mode, approval, and template readiness.";
  }
  if (text.includes("unauthorized") || text.includes("auth") || text.includes("api key")) {
    return "Open Settings and verify runtime API key configuration.";
  }
  if (text.includes("template")) {
    return "Open Workflows and fix template payload, interval, or enablement state.";
  }
  return undefined;
}

function buildRunsRoute(taskTypeHint?: string | null, taskId?: string | null, status = "failed"): string {
  const params = new URLSearchParams();
  if (taskId) {
    params.set("task_id", taskId);
  } else {
    if (status !== "all") params.set("status", status);
    if (taskTypeHint) params.set("task_type", taskTypeHint);
  }
  return `/runs?${params.toString()}`;
}

function dedupeActions(actions: FeedAction[]): FeedAction[] {
  const byKey = new Map<string, FeedAction>();
  actions.forEach((action) => {
    const key = `${action.label}|${action.to}`;
    if (!byKey.has(key)) byKey.set(key, action);
  });
  return Array.from(byKey.values());
}

function buildAlertActions(input: {
  category: AlertCategory;
  family: string;
  taskTypeHint?: string | null;
  taskId?: string | null;
  workflowRoute?: string | null;
}): FeedAction[] {
  const { category, family, taskTypeHint, taskId, workflowRoute } = input;
  const actions: FeedAction[] = [];

  const isSystem = category === "system" || family === "watchdog" || family === "stale-agent" || family === "stale";
  if (isSystem) {
    actions.push({ label: "Open Observability", to: "/observability", variant: "secondary" });
  }

  const hasRunsIntent =
    category === "actionNeeded" ||
    category === "workflow" ||
    family === "task-failure" ||
    family === "notify-failure" ||
    family === "budget";
  if (hasRunsIntent) {
    actions.push({ label: taskId ? "Inspect Latest Run" : "Open Matching Runs", to: buildRunsRoute(taskTypeHint, taskId), variant: "default" });
  }

  const workflowTarget = workflowRoute || (category === "workflow" || family === "notify-failure" ? "/workflows" : "");
  if (workflowTarget) {
    actions.push({ label: "Open Workflow Config", to: workflowTarget, variant: "outline" });
  }

  return dedupeActions(actions);
}

type JobsAlertDescriptor = {
  title: string;
  explanation: string;
  nextAction: string;
  family: string;
  severity?: AlertSeverity;
  category?: AlertCategory;
  actions: FeedAction[];
};

function buildJobsAlertActions(taskTypeHint: string, taskId: string | null, includeSourceCoverage: boolean, includeDigest: boolean, intentionalSkip = false): FeedAction[] {
  const actions: FeedAction[] = [
    {
      label: "Inspect Latest Run",
      to: buildRunsRoute(taskTypeHint, taskId, intentionalSkip ? "all" : "failed"),
      variant: "default"
    },
    { label: "Open Watcher Config", to: JOBS_WATCHER_ROUTE, variant: "outline" }
  ];

  if (includeSourceCoverage) {
    actions.push({ label: "Inspect Source Coverage", to: JOBS_SOURCE_COVERAGE_ROUTE, variant: "secondary" });
  }
  if (includeDigest) {
    actions.push({ label: "Inspect Digest Artifact", to: JOBS_DIGEST_ROUTE, variant: "secondary" });
  }

  return dedupeActions(actions);
}

function buildJobsAlertDescriptor(input: {
  taskTypeHint: string;
  message: string;
  metadata: Record<string, unknown>;
  taskId: string | null;
  defaultSeverity: AlertSeverity;
  defaultCategory: AlertCategory;
}): JobsAlertDescriptor | null {
  const { taskTypeHint, message, metadata, taskId, defaultSeverity, defaultCategory } = input;
  if (!isJobsTaskType(taskTypeHint)) return null;

  const text = `${taskTypeHint} ${message} ${asText(metadata.reason)} ${asText(metadata.summary)} ${asText(metadata.stage)}`.toLowerCase();
  const sourceHint = extractJobsSourceHint(metadata);
  const sourceLabel = sourceHint ? `: ${sourceHint}` : "";

  if (taskTypeHint === "jobs_digest_v2" && isIntentionalJobsNotifySkip(message, metadata)) {
    return {
      title: "Jobs notify skipped intentionally",
      explanation: "Digest completed without sending a notification because the shortlist was empty or notify policy said to skip.",
      nextAction: "Inspect the latest digest run to confirm the skip reason, then open watcher config if you expected a send.",
      family: "jobs-notify-skipped",
      severity: "info",
      category: "workflow",
      actions: buildJobsAlertActions(taskTypeHint, taskId, false, true, true)
    };
  }

  if ((taskTypeHint === "jobs_collect_v1" || taskTypeHint === "jobs_normalize_v1") && (text.includes("0 job") || text.includes("no result") || text.includes("empty result") || text.includes("empty set") || text.includes("no jobs"))) {
    return {
      title: taskTypeHint === "jobs_collect_v1" ? "Jobs collection returned no usable results" : "Jobs normalization collapsed to zero usable jobs",
      explanation: taskTypeHint === "jobs_collect_v1"
        ? "The jobs search ran but returned an empty or near-empty result set. This usually points to narrow query breadth or weak source coverage."
        : "Raw jobs were collected, but normalization and cleanup left no usable records. Inspect dedupe impact and metadata quality by source.",
      nextAction: "Inspect the latest run and source coverage, then widen titles, locations, or query count if the search scope is too narrow.",
      family: "jobs-empty-results",
      severity: defaultSeverity === "error" ? "error" : "warning",
      category: defaultCategory,
      actions: buildJobsAlertActions(taskTypeHint, taskId, true, false)
    };
  }

  if ((taskTypeHint === "jobs_collect_v1" || taskTypeHint === "jobs_normalize_v1") && (text.includes("metadata") || text.includes("missing_company") || text.includes("missing_posted_at") || text.includes("missing_source_url") || text.includes("malformed"))) {
    return {
      title: "Jobs metadata quality degraded",
      explanation: "Collected jobs are missing company, post date, direct links, or other key fields often enough to degrade downstream shortlist quality.",
      nextAction: "Inspect source coverage and metadata gap rates, then focus on the weakest adapter or extraction path.",
      family: "jobs-metadata-weak",
      severity: "warning",
      category: "workflow",
      actions: buildJobsAlertActions(taskTypeHint, taskId, true, false)
    };
  }

  if ((taskTypeHint === "jobs_collect_v1" || taskTypeHint === "jobs_normalize_v1") && (text.includes("source") || text.includes("adapter") || text.includes("timeout") || text.includes("rate limit"))) {
    return {
      title: `Jobs source coverage weak${sourceLabel}`,
      explanation: "One source adapter is returning weak, partial, or failing coverage, which can quietly shrink discovery breadth before ranking starts.",
      nextAction: "Inspect the latest run and source coverage to see which source underperformed and whether pagination, retries, or extraction need attention.",
      family: "jobs-source-weak",
      severity: defaultSeverity === "error" ? "error" : "warning",
      category: defaultCategory,
      actions: buildJobsAlertActions(taskTypeHint, taskId, true, false)
    };
  }

  if (taskTypeHint === "jobs_rank_v1") {
    return {
      title: "Jobs ranking failed",
      explanation: "The ranking stage could not score the normalized job set cleanly. This blocks shortlist quality even when search breadth looked healthy upstream.",
      nextAction: "Inspect the latest rank run for LLM attempts, fallback status, and input job quality before retrying.",
      family: "jobs-ranking-failure",
      severity: defaultSeverity,
      category: defaultCategory,
      actions: buildJobsAlertActions(taskTypeHint, taskId, false, true)
    };
  }

  if (taskTypeHint === "jobs_shortlist_v1") {
    return {
      title: "Jobs shortlist stage failed",
      explanation: "The shortlist step could not turn ranked jobs into a stable top set. This is usually a scoring, history, or cooldown-policy problem rather than source discovery.",
      nextAction: "Inspect the latest shortlist run, then review repeat/cooldown behavior and the ranked input quality.",
      family: "jobs-shortlist-failure",
      severity: defaultSeverity,
      category: defaultCategory,
      actions: buildJobsAlertActions(taskTypeHint, taskId, false, true)
    };
  }

  if (taskTypeHint === "jobs_digest_v2") {
    return {
      title: "Jobs digest generation failed",
      explanation: "Mission Control could not render the final jobs digest cleanly. Top-job selection may be ready, but the operator-facing summary or notification payload failed.",
      nextAction: "Inspect the latest digest run for model attempts, fallback status, and digest artifact output before retrying notify.",
      family: "jobs-digest-failure",
      severity: defaultSeverity,
      category: defaultCategory,
      actions: buildJobsAlertActions(taskTypeHint, taskId, false, true)
    };
  }

  return {
    title: `${taskTypeHint.replace(/_/g, " ")} needs review`,
    explanation: "A jobs pipeline stage raised a workflow alert. Inspect the latest run and stage-specific observability before adjusting watcher settings.",
    nextAction: "Inspect the latest run, then open watcher config if you need to widen search breadth or adjust shortlist policy.",
    family: "jobs-workflow-alert",
    severity: defaultSeverity,
    category: defaultCategory,
    actions: buildJobsAlertActions(taskTypeHint, taskId, taskTypeHint === "jobs_collect_v1" || taskTypeHint === "jobs_normalize_v1", taskTypeHint !== "jobs_collect_v1")
  };
}

function buildEventSignature(input: {
  event: EventOut;
  metadata: Record<string, unknown>;
  family: string;
  taskTypeHint: string | null;
}): string {
  const { event, metadata, family, taskTypeHint } = input;
  const normalizedMessage = normalizeForSignature(event.message || "");

  if (family === "watchdog") {
    const agent = inferAgentHint(event.message || "", metadata, event.source || "");
    const reason = normalizedMessage
      .replace(/\bwatchdog\b/g, "")
      .replace(/\bagent\b/g, "")
      .replace(/\bheartbeat\b/g, "")
      .trim();
    return `watchdog:${agent}:${reason || event.event_type}`;
  }
  if (family === "stale-agent") {
    return `stale-agent:${inferAgentHint(event.message || "", metadata, event.source || "")}`;
  }
  if (family === "notify-failure") {
    const notifyTarget = asText(metadata.channel) || asText(metadata.channels) || "unknown_channel";
    return `notify-failure:${taskTypeHint || "notify_v1"}:${normalizeForSignature(notifyTarget)}`;
  }
  if (family === "planner-tick" || family === "planner-cycle" || family === "budget") {
    return family;
  }
  if (family === "task-failure") {
    return `task-failure:${taskTypeHint || "unknown"}:${normalizeForSignature(event.event_type)}`;
  }
  return `${event.event_type}|${event.source}|${normalizedMessage}`;
}

function scoreSignal(signal: Pick<AlertSignal, "severity" | "category" | "createdAtMs" | "count">): number {
  const now = Date.now();
  const ageMs = Math.max(0, now - signal.createdAtMs);
  const freshness = ageMs <= 15 * 60 * 1000 ? 25 : ageMs <= 60 * 60 * 1000 ? 10 : 0;
  const repetition = signal.count && signal.count > 1 ? Math.min(signal.count * 5, 40) : 0;
  return severityWeight(signal.severity) + categoryWeight(signal.category) + freshness + repetition;
}

function buildEventSignals(events: EventOut[]): AlertSignal[] {
  return events.map((event, index) => {
    const metadata = parseMetadataRecord(event.metadata_json);
    const taskTypeHint = inferTaskTypeHint(event.event_type, event.message || "", metadata);
    const taskId = extractTaskIdHint(metadata);
    const severity = inferSeverity(event);
    const category = inferCategory(event, severity);
    const family = classifyFamily(event.event_type, event.message, taskTypeHint);
    const workflowRoute = inferWorkflowRoute(taskTypeHint, event.event_type, event.message || "");
    const createdAtMs = parseTimestampMs(event.created_at);
    const jobsDescriptor =
      taskTypeHint && isJobsTaskType(taskTypeHint)
        ? buildJobsAlertDescriptor({
            taskTypeHint,
            message: event.message || "",
            metadata,
            taskId,
            defaultSeverity: severity,
            defaultCategory: category
          })
        : null;
    const finalSeverity = jobsDescriptor?.severity || severity;
    const finalCategory = jobsDescriptor?.category || category;
    const finalFamily = jobsDescriptor?.family || family;

    const signal: AlertSignal = {
      id: event.id || `${event.event_type}-${event.created_at}-${index}`,
      title: jobsDescriptor?.title || humanizeEventType(event.event_type),
      explanation: jobsDescriptor?.explanation || summarizeText(event.message || ""),
      source: event.source,
      level: finalSeverity,
      createdAt: event.created_at,
      nextAction: jobsDescriptor?.nextAction || inferNextAction(event.event_type, event.message || "", family),
      category: finalCategory,
      severity: finalSeverity,
      signature: buildEventSignature({ event, metadata, family: finalFamily, taskTypeHint }),
      priority: 0,
      createdAtMs,
      family: finalFamily,
      count: 1,
      taskTypeHint,
      workflowRoute,
      actions: jobsDescriptor?.actions || buildAlertActions({ category: finalCategory, family: finalFamily, taskTypeHint, taskId, workflowRoute })
    };

    signal.priority = scoreSignal(signal);
    return signal;
  });
}

function buildTaskSignals(tasks: TaskOut[]): AlertSignal[] {
  return tasks
    .filter((task) => FAILING_TASK_STATUSES.has(task.status))
    .map((task) => {
      const severity: AlertSeverity = task.status === "failed" ? "warning" : "error";
      const category: AlertCategory = task.status === "failed" ? "workflow" : "actionNeeded";
      const createdAtMs = parseTimestampMs(task.updated_at);
      const family = task.task_type === "notify_v1" ? "notify-failure" : "task-failure";
      const workflowRoute = PRIMARY_WORKFLOW_ROUTE_BY_TASK_TYPE[task.task_type] || (family === "notify-failure" ? "/workflows" : null);
      const jobsDescriptor = buildJobsAlertDescriptor({
        taskTypeHint: task.task_type,
        message: task.diagnostics?.summary || task.error || "",
        metadata: {},
        taskId: task.id,
        defaultSeverity: severity,
        defaultCategory: category
      });
      const finalSeverity = jobsDescriptor?.severity || severity;
      const finalCategory = jobsDescriptor?.category || category;
      const finalFamily = jobsDescriptor?.family || family;

      const signal: AlertSignal = {
        id: `task-${task.id}`,
        title: jobsDescriptor?.title || `${task.task_type} ${task.status.replace(/_/g, " ")}`,
        explanation: jobsDescriptor?.explanation || summarizeText(task.diagnostics?.summary || task.error || "Task requires operator review."),
        source: "task-runner",
        level: finalSeverity,
        createdAt: task.updated_at,
        nextAction: jobsDescriptor?.nextAction ||
          (
          family === "notify-failure"
            ? "Open workflow config, verify channel/dedupe settings, then retry failed notification runs."
            : "Open matching runs and inspect attempts plus result artifacts."
          ),
        category: finalCategory,
        severity: finalSeverity,
        signature:
          finalFamily === "notify-failure"
            ? `task:notify-failure:${task.status}`
            : `task:${task.task_type}:${task.status}:${normalizeForSignature(task.diagnostics?.summary || task.error || "")}`,
        priority: 0,
        createdAtMs,
        family: finalFamily,
        count: 1,
        taskTypeHint: task.task_type,
        workflowRoute,
        actions: jobsDescriptor?.actions || buildAlertActions({ category: finalCategory, family: finalFamily, taskTypeHint: task.task_type, taskId: task.id, workflowRoute })
      };
      signal.priority = scoreSignal(signal);
      return signal;
    });
}

function mergeAndCompress(signals: AlertSignal[]): AlertSignal[] {
  const bySignature = new Map<string, AlertSignal[]>();
  signals.forEach((signal) => {
    const arr = bySignature.get(signal.signature) || [];
    arr.push(signal);
    bySignature.set(signal.signature, arr);
  });

  const merged = Array.from(bySignature.values()).map((group) => {
    const sorted = [...group].sort((a, b) => b.createdAtMs - a.createdAtMs);
    const latest = { ...sorted[0] };

    if (sorted.length === 1) {
      latest.priority = scoreSignal(latest);
      return latest;
    }

    const severity = sorted.some((item) => item.severity === "error")
      ? "error"
      : sorted.some((item) => item.severity === "warning")
        ? "warning"
        : "info";

    const category = sorted.some((item) => item.category === "actionNeeded")
      ? "actionNeeded"
      : sorted.some((item) => item.category === "workflow")
        ? "workflow"
        : sorted.some((item) => item.category === "system")
          ? "system"
          : "informational";

    const nextAction = sorted.find((item) => item.nextAction)?.nextAction;
    const actions = dedupeActions(sorted.flatMap((item) => item.actions || []));
    const compressedExplanation =
      latest.family === "watchdog" || latest.family === "stale-agent"
        ? `${sorted.length} repeated watchdog/heartbeat alerts grouped. Latest: ${latest.explanation || "No detail."}`
        : latest.family === "notify-failure"
          ? `${sorted.length} repeated notification delivery failures grouped. Latest: ${latest.explanation || "No detail."}`
          : `Repeated ${sorted.length} similar alerts grouped. Latest: ${latest.explanation || "No detail."}`;

    const compressed: AlertSignal = {
      ...latest,
      level: severity,
      severity,
      category,
      count: sorted.length,
      explanation: compressedExplanation,
      nextAction,
      actions,
      priority: 0
    };
    compressed.priority = scoreSignal(compressed);
    return compressed;
  });

  return merged.filter((signal) => {
    const lowValue = signal.family === "planner-tick" || signal.family === "planner-cycle";
    if (!lowValue) return true;
    return (signal.count || 1) > 2 || signal.category === "actionNeeded";
  });
}

function sortSignals(signals: AlertSignal[]): AlertSignal[] {
  return [...signals].sort((a, b) => {
    if (a.priority !== b.priority) return b.priority - a.priority;
    return b.createdAtMs - a.createdAtMs;
  });
}

function toFeedRows(signals: AlertSignal[]): FeedEvent[] {
  return signals.map((signal) => ({
    id: signal.id,
    title: signal.title,
    explanation: signal.explanation,
    source: signal.source,
    level: signal.level,
    createdAt: signal.createdAt,
    nextAction: signal.nextAction,
    count: signal.count,
    actions: signal.actions
  }));
}

export function AlertsPage(): JSX.Element {
  const eventsQuery = useTelemetryEvents(350);
  const tasksQuery = useTasks(180);
  const [focus, setFocus] = useState<AlertFocus>("all");

  const grouped = useMemo(() => {
    const eventSignals = buildEventSignals(eventsQuery.data || []);
    const taskSignals = buildTaskSignals(tasksQuery.data || []);
    const consolidated = sortSignals(mergeAndCompress([...eventSignals, ...taskSignals]));

    const actionNeededAll = consolidated.filter((item) => item.category === "actionNeeded");
    const workflowAll = consolidated.filter((item) => item.category === "workflow");
    const systemAll = consolidated.filter((item) => item.category === "system");
    const informationalAll = consolidated.filter((item) => item.category === "informational");

    return {
      actionNeeded: toFeedRows(actionNeededAll.slice(0, 10)),
      workflow: toFeedRows(workflowAll.slice(0, 12)),
      system: toFeedRows(systemAll.slice(0, 12)),
      informational: toFeedRows(informationalAll.slice(0, 12)),
      counts: {
        actionNeeded: actionNeededAll.length,
        workflow: workflowAll.length,
        system: systemAll.length,
        informational: informationalAll.length
      },
      highSignalCount: actionNeededAll.length + workflowAll.length + systemAll.length
    };
  }, [eventsQuery.data, tasksQuery.data]);

  const pageError = [eventsQuery.error, tasksQuery.error].find(Boolean);
  const loading = eventsQuery.isLoading || tasksQuery.isLoading;
  const retryAll = (): void => {
    void Promise.all([eventsQuery.refetch(), tasksQuery.refetch()]);
  };

  const showActionNeeded = focus === "all" || focus === "actionNeeded";
  const showWorkflow = focus === "all" || focus === "workflow";
  const showSystem = focus === "all" || focus === "system";
  const showInformational = focus === "all";

  return (
    <div className="space-y-4">
      <PageHeader
        title="Alerts"
        subtitle="High-signal operational issues with direct actions to resolve faster."
        actions={<Button variant="secondary" onClick={retryAll}>Refresh Alerts</Button>}
      />
      {pageError ? <ErrorPanel title="Alerts failed to load" message={errorMessage(pageError)} onAction={retryAll} /> : null}

      <section>
        <Card>
          <CardContent className="space-y-3 p-4">
            <SectionHeader title="Quick Focus" subtitle="Jump to a lane or focus only one alert queue." />
            <div className="flex flex-wrap gap-2">
              <Button size="sm" variant={focus === "all" ? "default" : "secondary"} onClick={() => setFocus("all")}>
                All High-Signal ({grouped.highSignalCount})
              </Button>
              <Button size="sm" variant={focus === "actionNeeded" ? "default" : "secondary"} onClick={() => setFocus("actionNeeded")}>
                Action Needed ({grouped.counts.actionNeeded})
              </Button>
              <Button size="sm" variant={focus === "workflow" ? "default" : "secondary"} onClick={() => setFocus("workflow")}>
                Workflow Alerts ({grouped.counts.workflow})
              </Button>
              <Button size="sm" variant={focus === "system" ? "default" : "secondary"} onClick={() => setFocus("system")}>
                System Alerts ({grouped.counts.system})
              </Button>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button size="sm" variant="outline" asChild><a href="#alerts-action-needed">Action Needed</a></Button>
              <Button size="sm" variant="outline" asChild><a href="#alerts-workflow">Workflow Alerts</a></Button>
              <Button size="sm" variant="outline" asChild><a href="#alerts-system">System Alerts</a></Button>
            </div>
            <div className="text-xs text-muted-foreground">
              Repeated watchdog/stale-agent and notify failures are grouped into single alerts with occurrence counts.
            </div>
          </CardContent>
        </Card>
      </section>

      {showActionNeeded ? (
        <section id="alerts-action-needed">
          <Card>
            <CardContent className="p-4">
              <SectionHeader title="Action Needed" subtitle="Highest-priority alerts requiring operator response now." />
              {grouped.actionNeeded.length === 0 && !loading ? (
                <EmptyState
                  title="No urgent issues"
                  description="No critical failures or blocked workflows are currently detected."
                />
              ) : (
                <EventFeedList rows={grouped.actionNeeded} loading={loading} emptyText="No urgent action items." />
              )}
            </CardContent>
          </Card>
        </section>
      ) : null}

      <div className={showWorkflow && showSystem ? "grid gap-4 xl:grid-cols-2" : "grid gap-4"}>
        {showWorkflow ? (
          <Card id="alerts-workflow">
            <CardContent className="p-4">
              <SectionHeader title="Workflow Alerts" subtitle="Planner/template/task execution signals and degradations." />
              {grouped.workflow.length === 0 && !loading ? (
                <EmptyState
                  title="No active workflow issues"
                  description="Planner and workflow operations look stable in the current event window."
                />
              ) : (
                <EventFeedList rows={grouped.workflow} loading={loading} emptyText="No workflow alerts." />
              )}
            </CardContent>
          </Card>
        ) : null}

        {showSystem ? (
          <Card id="alerts-system">
            <CardContent className="p-4">
              <SectionHeader title="System Alerts" subtitle="Infrastructure health, watchdog, and stale-agent conditions." />
              {grouped.system.length === 0 && !loading ? (
                <EmptyState
                  title="No system degradation"
                  description="Watchdog and heartbeat streams show no active system warnings."
                />
              ) : (
                <EventFeedList rows={grouped.system} loading={loading} emptyText="No system alerts." />
              )}
            </CardContent>
          </Card>
        ) : null}

        {showInformational ? (
          <Card className="xl:col-span-2">
            <CardContent className="p-4">
              <SectionHeader title="Informational / Recent" subtitle="Lower-priority background signals and recent context." />
              {grouped.informational.length === 0 && !loading ? (
                <EmptyState
                  title="No informational noise"
                  description="Low-value repetitive informational events are currently suppressed."
                />
              ) : (
                <EventFeedList rows={grouped.informational} loading={loading} emptyText="No informational events." />
              )}
            </CardContent>
          </Card>
        ) : null}
      </div>
    </div>
  );
}
