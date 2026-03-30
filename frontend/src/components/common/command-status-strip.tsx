import { useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowRight, Play, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "@/components/common/status-badge";
import { useApiRuntime } from "@/app/providers";
import { useRunPlannerOnceMutation } from "@/features/planner/queries";
import { useHealthStatus, usePlannerStatus, useReadyStatus } from "@/features/telemetry/queries";
import type { PlannerStatusOut } from "@/lib/api/generated/openapi";
import { errorMessage } from "@/lib/utils/errors";
import { formatIso } from "@/lib/utils/format";

export type CommandStripState = {
  healthLabel: string;
  readyLabel: string;
  plannerModeLabel: string;
  plannerApprovalLabel: string;
  apiKeyLabel: string;
};

export function deriveCommandStripState(input: {
  health: { status?: string } | null;
  ready: { status?: string } | null;
  planner: PlannerStatusOut | null;
  apiKeyPresent: boolean;
}): CommandStripState {
  const healthStatus = String(input.health?.status || "unknown").toLowerCase();
  const readyStatus = String(input.ready?.status || "unknown").toLowerCase();
  const planner = input.planner;

  return {
    healthLabel: healthStatus,
    readyLabel: readyStatus,
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
  const runOnceMutation = useRunPlannerOnceMutation();
  const stripError = [healthQuery.error, readyQuery.error, plannerQuery.error].find(Boolean);

  const state = deriveCommandStripState({
    health: healthQuery.data || null,
    ready: readyQuery.data || null,
    planner: plannerQuery.data || null,
    apiKeyPresent: Boolean(apiKey.trim())
  });

  const lastRefreshAt = useMemo(() => {
    const stamps = [healthQuery.dataUpdatedAt, readyQuery.dataUpdatedAt, plannerQuery.dataUpdatedAt].filter((n) => Number.isFinite(n) && n > 0);
    if (stamps.length === 0) return null;
    return new Date(Math.max(...stamps));
  }, [healthQuery.dataUpdatedAt, plannerQuery.dataUpdatedAt, readyQuery.dataUpdatedAt]);

  return (
    <div className="border-b border-border/70 bg-card/90">
      <div className="container flex flex-wrap items-center justify-between gap-2 py-2">
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="font-medium uppercase tracking-[0.09em] text-muted-foreground">health</span>
          <StatusBadge status={state.healthLabel} />
          <span className="font-medium uppercase tracking-[0.08em] text-muted-foreground">ready</span>
          <StatusBadge status={state.readyLabel} />
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
