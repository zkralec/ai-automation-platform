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

function buildRunsRoute(taskTypeHint?: string | null): string {
  const params = new URLSearchParams();
  params.set("status", "failed");
  if (taskTypeHint) params.set("task_type", taskTypeHint);
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
  workflowRoute?: string | null;
}): FeedAction[] {
  const { category, family, taskTypeHint, workflowRoute } = input;
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
    actions.push({ label: "Open Matching Runs", to: buildRunsRoute(taskTypeHint), variant: "default" });
  }

  const workflowTarget = workflowRoute || (category === "workflow" || family === "notify-failure" ? "/workflows" : "");
  if (workflowTarget) {
    actions.push({ label: "Open Workflow Config", to: workflowTarget, variant: "outline" });
  }

  return dedupeActions(actions);
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
    const severity = inferSeverity(event);
    const category = inferCategory(event, severity);
    const family = classifyFamily(event.event_type, event.message, taskTypeHint);
    const workflowRoute = inferWorkflowRoute(taskTypeHint, event.event_type, event.message || "");
    const createdAtMs = parseTimestampMs(event.created_at);

    const signal: AlertSignal = {
      id: event.id || `${event.event_type}-${event.created_at}-${index}`,
      title: humanizeEventType(event.event_type),
      explanation: summarizeText(event.message || ""),
      source: event.source,
      level: severity,
      createdAt: event.created_at,
      nextAction: inferNextAction(event.event_type, event.message || "", family),
      category,
      severity,
      signature: buildEventSignature({ event, metadata, family, taskTypeHint }),
      priority: 0,
      createdAtMs,
      family,
      count: 1,
      taskTypeHint,
      workflowRoute,
      actions: buildAlertActions({ category, family, taskTypeHint, workflowRoute })
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

      const signal: AlertSignal = {
        id: `task-${task.id}`,
        title: `${task.task_type} ${task.status.replace(/_/g, " ")}`,
        explanation: summarizeText(task.error || "Task requires operator review."),
        source: "task-runner",
        level: severity,
        createdAt: task.updated_at,
        nextAction:
          family === "notify-failure"
            ? "Open workflow config, verify channel/dedupe settings, then retry failed notification runs."
            : "Open matching runs and inspect attempts plus result artifacts.",
        category,
        severity,
        signature:
          family === "notify-failure"
            ? `task:notify-failure:${task.status}`
            : `task:${task.task_type}:${task.status}:${normalizeForSignature(task.error || "")}`,
        priority: 0,
        createdAtMs,
        family,
        count: 1,
        taskTypeHint: task.task_type,
        workflowRoute,
        actions: buildAlertActions({ category, family, taskTypeHint: task.task_type, workflowRoute })
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
