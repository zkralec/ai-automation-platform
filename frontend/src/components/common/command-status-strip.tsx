import { useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowRight, Play, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "@/components/common/status-badge";
import { useApiRuntime } from "@/app/providers";
import { useRunPlannerOnceMutation } from "@/features/planner/queries";
import { useHealthStatus, usePlannerStatus, useReadyStatus, useRuntimeStatus } from "@/features/telemetry/queries";
import type { PlannerStatusOut, RuntimeStatusOut } from "@/lib/api/generated/openapi";
import { errorMessage } from "@/lib/utils/errors";
import { formatIso } from "@/lib/utils/format";

export type CommandStripState = {
  healthLabel: string;
  readyLabel: string;
  apiLabel: string;
  schedulerLabel: string;
  workerLabel: string;
  redisLabel: string;
  queueDepthLabel: string;
  lastSchedulerTickLabel: string;
  plannerModeLabel: string;
  plannerApprovalLabel: string;
  apiKeyLabel: string;
};

export function deriveCommandStripState(input: {
  health: { status?: string } | null;
  ready: { status?: string } | null;
  planner: PlannerStatusOut | null;
  runtime: RuntimeStatusOut | null;
  apiKeyPresent: boolean;
}): CommandStripState {
  const healthStatus = String(input.health?.status || "unknown").toLowerCase();
  const readyStatus = String(input.ready?.status || "unknown").toLowerCase();
  const planner = input.planner;
  const runtime = input.runtime;

  return {
    healthLabel: healthStatus,
    readyLabel: readyStatus,
    apiLabel: runtime ? (runtime.api_healthy ? "healthy" : "degraded") : "unknown",
    schedulerLabel: runtime?.scheduler_heartbeat ? (runtime.scheduler_heartbeat.healthy ? "alive" : runtime.scheduler_heartbeat.status || "stale") : "unknown",
    workerLabel: runtime?.worker_heartbeat ? (runtime.worker_heartbeat.healthy ? "alive" : runtime.worker_heartbeat.status || "stale") : "unknown",
    redisLabel: runtime ? (runtime.redis_reachable ? "reachable" : "unreachable") : "unknown",
    queueDepthLabel: runtime && typeof runtime.queue_depth === "number" ? String(runtime.queue_depth) : "-",
    lastSchedulerTickLabel: runtime?.last_scheduler_tick_at ? formatIso(runtime.last_scheduler_tick_at) : "-",
    plannerModeLabel: planner ? String(planner.mode || "unknown") : "unknown",
    plannerApprovalLabel: planner ? (planner.require_approval ? (planner.approved ? "approved" : "awaiting") : "not_required") : "unknown",
    apiKeyLabel: input.apiKeyPresent ? "present" : "missing"
  };
}

export function CommandStatusStrip(): JSX.Element {
  const navigate = useNavigate();
  const { apiKey } = useApiRuntime();

  const healthQuery = useHealthStatus();
  const readyQuery = useReadyStatus();
  const plannerQuery = usePlannerStatus();
  const runtimeQuery = useRuntimeStatus();
  const runOnceMutation = useRunPlannerOnceMutation();
  const stripError = [healthQuery.error, readyQuery.error, plannerQuery.error, runtimeQuery.error].find(Boolean);

  const state = deriveCommandStripState({
    health: healthQuery.data || null,
    ready: readyQuery.data || null,
    planner: plannerQuery.data || null,
    runtime: runtimeQuery.data || null,
    apiKeyPresent: Boolean(apiKey.trim())
  });

  const lastRefreshAt = useMemo(() => {
    const stamps = [healthQuery.dataUpdatedAt, readyQuery.dataUpdatedAt, plannerQuery.dataUpdatedAt, runtimeQuery.dataUpdatedAt].filter((n) => Number.isFinite(n) && n > 0);
    if (stamps.length === 0) return null;
    return new Date(Math.max(...stamps));
  }, [healthQuery.dataUpdatedAt, plannerQuery.dataUpdatedAt, readyQuery.dataUpdatedAt, runtimeQuery.dataUpdatedAt]);

  return (
    <div className="border-b border-border/70 bg-card/90">
      <div className="container flex flex-wrap items-center justify-between gap-2 py-2">
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="font-medium uppercase tracking-[0.09em] text-muted-foreground">api</span>
          <StatusBadge status={state.apiLabel} />
          <span className="font-medium uppercase tracking-[0.09em] text-muted-foreground">health</span>
          <StatusBadge status={state.healthLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">ready</span>
          <StatusBadge status={state.readyLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">scheduler</span>
          <StatusBadge status={state.schedulerLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">worker</span>
          <StatusBadge status={state.workerLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">redis</span>
          <StatusBadge status={state.redisLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">queue</span>
          <span className="text-foreground">{state.queueDepthLabel}</span>
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">last tick</span>
          <span className="text-muted-foreground">{state.lastSchedulerTickLabel}</span>
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">api key</span>
          <StatusBadge status={state.apiKeyLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">planner mode</span>
          <StatusBadge status={state.plannerModeLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">approval</span>
          <StatusBadge status={state.plannerApprovalLabel} />
          <span className="ml-2 text-muted-foreground">last refresh: {lastRefreshAt ? formatIso(lastRefreshAt.toISOString()) : "-"}</span>
          {stripError ? <span className="text-destructive">status degraded: {errorMessage(stripError)}</span> : null}
          {runOnceMutation.error ? <span className="text-destructive">run-once failed: {errorMessage(runOnceMutation.error)}</span> : null}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button size="sm" variant="secondary" onClick={() => navigate("/workflows?create=1")}>
            <Plus className="h-3.5 w-3.5" />
            New Workflow Run
          </Button>
          <Button size="sm" variant="secondary" onClick={() => navigate("/runs?status=failed")}>
            <ArrowRight className="h-3.5 w-3.5" />
            Go to Recent Failed Runs
          </Button>
          <Button size="sm" onClick={() => runOnceMutation.mutate()} disabled={runOnceMutation.isPending}>
            <Play className="h-3.5 w-3.5" />
            {runOnceMutation.isPending ? "Running..." : "Run Planner Once"}
          </Button>
        </div>
      </div>
    </div>
  );
}
