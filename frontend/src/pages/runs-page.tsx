import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { DataTableWrapper } from "@/components/common/data-table-wrapper";
import { DetailsSurface } from "@/components/common/details-surface";
import { EmptyState } from "@/components/common/empty-state";
import { ErrorPanel } from "@/components/common/error-panel";
import { JsonViewer } from "@/components/common/json-viewer";
import { PageHeader } from "@/components/common/page-header";
import { SectionHeader } from "@/components/common/section-header";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useRuns, useTask, useTaskResult, useTaskRuns, useTasks } from "@/features/tasks/queries";
import type { RunOut, TaskOut, TaskResultOut } from "@/lib/api/generated/openapi";
import { errorMessage } from "@/lib/utils/errors";
import { formatCost, formatDurationMs, formatIso } from "@/lib/utils/format";

const JOBS_WATCHER_ROUTE = "/workflows?watcher=preset-jobs-digest-scan";

type JobsPreviewAction = {
  label: string;
  to: string;
};

type RunFailureMode = "retryable" | "permanent" | null;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asText(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return null;
}

function parseMaybeJsonText(raw: string): unknown {
  const trimmed = raw.trim();
  if (!trimmed) return "";
  try {
    return JSON.parse(trimmed);
  } catch {
    return raw;
  }
}

function parseTaskPayload(task: TaskOut | undefined): unknown | null {
  if (!task) return null;
  try {
    return JSON.parse(task.payload_json);
  } catch {
    return task.payload_json;
  }
}

function resolveResultPayload(result: TaskResultOut | null | undefined): unknown | null {
  if (!result) return null;
  if (result.content_json !== undefined && result.content_json !== null) return result.content_json;
  if (result.content_text) return parseMaybeJsonText(result.content_text);
  return null;
}

function timestampLabel(value: string | null | undefined): JSX.Element {
  if (!value) {
    return <span className="text-muted-foreground">-</span>;
  }
  return (
    <div className="space-y-0.5">
      <div>{formatIso(value)}</div>
      <div className="font-mono text-[10px] text-muted-foreground">{value}</div>
    </div>
  );
}

function toRecordArray(value: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) return [];
  return value.filter((row) => isRecord(row)) as Array<Record<string, unknown>>;
}

function pickRecordArray(payload: unknown, keys: string[]): Array<Record<string, unknown>> {
  if (Array.isArray(payload)) return toRecordArray(payload);
  if (!isRecord(payload)) return [];

  for (const key of keys) {
    const direct = payload[key];
    const rows = toRecordArray(direct);
    if (rows.length > 0) return rows;
  }

  if (isRecord(payload.data)) {
    for (const key of keys) {
      const nested = payload.data[key];
      const rows = toRecordArray(nested);
      if (rows.length > 0) return rows;
    }
  }

  return [];
}

function runFailureMode(run: RunOut, task: TaskOut | undefined): RunFailureMode {
  if (run.status !== "failed") return null;
  if (!task) return "retryable";
  if (task.status === "failed_permanent") return "permanent";
  return run.attempt >= task.max_attempts ? "permanent" : "retryable";
}

function taskFailureMode(task: TaskOut, attemptCount: number): RunFailureMode {
  if (task.status === "failed_permanent") return "permanent";
  if (task.status === "failed") {
    return attemptCount >= task.max_attempts ? "permanent" : "retryable";
  }
  return null;
}

function describeDiagnostics(task: TaskOut | undefined): string | null {
  if (!task?.diagnostics) return task?.error || null;
  const bits = [task.diagnostics.summary];
  if (task.diagnostics.upstream_service) bits.push(`upstream=${task.diagnostics.upstream_service}`);
  if (task.diagnostics.source) bits.push(`source=${task.diagnostics.source}`);
  if (task.diagnostics.stage) bits.push(`stage=${task.diagnostics.stage}`);
  return bits.filter(Boolean).join(" · ");
}

function PreviewFallback({ payload }: { payload: unknown }): JSX.Element {
  if (payload === null || payload === undefined) {
    return <EmptyState title="No result preview available" description="This run has no structured result payload yet." />;
  }

  if (typeof payload === "string") {
    return <div className="rounded border border-border bg-muted/20 p-3 text-xs whitespace-pre-wrap">{payload.slice(0, 1600)}</div>;
  }

  if (Array.isArray(payload)) {
    return (
      <div className="space-y-2 rounded border border-border bg-muted/20 p-3 text-xs">
        <div>Result array with {payload.length} items.</div>
        <div className="text-muted-foreground">Open Raw JSON for full inspection.</div>
      </div>
    );
  }

  if (isRecord(payload)) {
    const previewEntries = Object.entries(payload)
      .filter(([, value]) => ["string", "number", "boolean"].includes(typeof value))
      .slice(0, 10);

    if (previewEntries.length === 0) {
      return <div className="rounded border border-border bg-muted/20 p-3 text-xs">Structured payload detected. Open Raw JSON for full detail.</div>;
    }

    return (
      <div className="grid gap-2 sm:grid-cols-2">
        {previewEntries.map(([key, value]) => (
          <div key={key} className="rounded border border-border bg-card p-2 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">{key}</div>
            <div className="mt-1 break-all">{String(value)}</div>
          </div>
        ))}
      </div>
    );
  }

  return <div className="rounded border border-border bg-muted/20 p-3 text-xs">Result payload is not previewable in structured form.</div>;
}

function NotifyPreview({ resultPayload, taskPayload }: { resultPayload: unknown; taskPayload: unknown | null }): JSX.Element {
  const source = isRecord(resultPayload) ? resultPayload : isRecord(taskPayload) ? taskPayload : null;
  if (!source) return <PreviewFallback payload={resultPayload} />;

  const channels = Array.isArray(source.channels)
    ? source.channels.map((row) => (typeof row === "string" ? row : "")).filter(Boolean).join(", ")
    : "";
  const providerResult = isRecord(source.provider_result) ? source.provider_result : null;
  const channel = channels || asText(source.channel) || "-";
  const delivery =
    asText(source.delivery_status) ||
    asText(source.status) ||
    asText(providerResult?.status) ||
    (source.sent === true ? "sent" : source.sent === false ? "not_sent" : "unknown");
  const dedupe = asText(source.dedupe_key) || asText(source.idempotency_key) || "-";
  const message =
    asText(source.message) ||
    asText(source.message_preview) ||
    asText(source.content) ||
    asText(source.text) ||
    "No message field found.";

  return (
    <div className="space-y-3">
      <div className="grid gap-2 sm:grid-cols-3">
        <div className="rounded border border-border bg-card p-2 text-xs">
          <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Channel</div>
          <div className="mt-1">{channel}</div>
        </div>
        <div className="rounded border border-border bg-card p-2 text-xs">
          <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Delivery</div>
          <div className="mt-1"><StatusBadge status={delivery} /></div>
        </div>
        <div className="rounded border border-border bg-card p-2 text-xs">
          <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Dedupe Key</div>
          <div className="mt-1 break-all">{dedupe}</div>
        </div>
      </div>

      <div className="rounded border border-border bg-muted/20 p-3 text-xs whitespace-pre-wrap">
        {message}
      </div>
    </div>
  );
}

function DealsPreview({ resultPayload }: { resultPayload: unknown }): JSX.Element {
  const deals = pickRecordArray(resultPayload, ["deals", "items", "results", "opportunities", "matches"]);
  const alerts = pickRecordArray(resultPayload, ["alerts", "notifications", "unicorn_alerts"]);

  if (deals.length === 0 && alerts.length === 0) {
    return <PreviewFallback payload={resultPayload} />;
  }

  return (
    <div className="space-y-3">
      <div className="grid gap-2 sm:grid-cols-3">
        <div className="rounded border border-border bg-card p-2 text-xs">
          <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Deals</div>
          <div className="mt-1 text-sm font-semibold">{deals.length}</div>
        </div>
        <div className="rounded border border-border bg-card p-2 text-xs">
          <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Alerts</div>
          <div className="mt-1 text-sm font-semibold">{alerts.length}</div>
        </div>
        <div className="rounded border border-border bg-card p-2 text-xs">
          <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Preview Rows</div>
          <div className="mt-1 text-sm font-semibold">{Math.min(5, deals.length || alerts.length)}</div>
        </div>
      </div>

      {deals.length > 0 ? (
        <div className="space-y-2 rounded border border-border bg-card p-3">
          {deals.slice(0, 5).map((row, idx) => {
            const title = asText(row.title) || asText(row.name) || asText(row.product) || `Deal #${idx + 1}`;
            const source = asText(row.source) || asText(row.store) || asText(row.vendor) || "unknown";
            const price = asText(row.price) || asText(row.deal_price) || asText(row.amount) || "-";
            const url = asText(row.url) || asText(row.link);
            return (
              <div key={`${title}-${idx}`} className="rounded border border-border/80 bg-muted/20 px-2 py-2 text-xs">
                <div className="font-medium">{title}</div>
                <div className="mt-1 text-muted-foreground">source: {source} · price: {price}</div>
                {url ? (
                  <a className="mt-1 inline-block break-all text-primary underline" href={url} target="_blank" rel="noreferrer">
                    {url}
                  </a>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : null}

      {alerts.length > 0 ? (
        <div className="rounded border border-border bg-muted/20 p-3 text-xs">
          <div className="font-medium">Alert sample</div>
          <div className="mt-1 text-muted-foreground break-words">
            {asText(alerts[0].message) || asText(alerts[0].title) || "Alert payload present."}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function formatPercent(value: unknown): string {
  const parsed = asNumber(value);
  if (parsed === null) return "0%";
  return `${Math.round(parsed)}%`;
}

function observabilityRecord(payload: unknown, key: string): Record<string, unknown> | null {
  if (!isRecord(payload)) return null;
  const value = payload[key];
  return isRecord(value) ? value : null;
}

function buildRunsTaskLink(taskId: string | null | undefined): string {
  if (!taskId) return "/runs";
  const params = new URLSearchParams();
  params.set("task_id", taskId);
  return `/runs?${params.toString()}`;
}

function buildRunsTaskTypeLink(taskType: string, status?: string): string {
  const params = new URLSearchParams();
  params.set("task_type", taskType);
  if (status) params.set("status", status);
  return `/runs?${params.toString()}`;
}

function renderJobsPreviewActions(actions: JobsPreviewAction[]): JSX.Element | null {
  if (actions.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-2">
      {actions.map((action) => (
        <Button key={`${action.label}-${action.to}`} asChild size="sm" variant="outline">
          <Link to={action.to}>{action.label}</Link>
        </Button>
      ))}
    </div>
  );
}

function jobsStageActions(taskType: string, taskId: string | null | undefined): JobsPreviewAction[] {
  const actions: JobsPreviewAction[] = [
    { label: "Inspect Latest Run", to: buildRunsTaskLink(taskId) },
    { label: "Open Watcher Config", to: JOBS_WATCHER_ROUTE }
  ];

  if (taskType === "jobs_collect_v1" || taskType === "jobs_normalize_v1") {
    actions.push({ label: "Inspect Source Coverage", to: buildRunsTaskTypeLink("jobs_collect_v1") });
  }
  if (taskType === "jobs_digest_v2" || taskType === "jobs_shortlist_v1") {
    actions.push({ label: "Inspect Digest Artifact", to: buildRunsTaskTypeLink("jobs_digest_v2") });
  }
  return actions;
}

function jobsPreviewRows(resultPayload: unknown): Array<Record<string, unknown>> {
  return pickRecordArray(resultPayload, [
    "jobs",
    "openings",
    "results",
    "items",
    "matches",
    "opportunities",
    "raw_jobs",
    "normalized_jobs",
    "ranked_jobs",
    "shortlist",
    "top_jobs",
    "digest_jobs"
  ]);
}

function textArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((row) => asText(row)?.trim() || "").filter(Boolean);
}

function isInactiveSourceCoverageRow(value: Record<string, unknown>): boolean {
  const sourceErrorType = asText(value.source_error_type);
  const status = asText(value.status);
  return sourceErrorType === "source_disabled" || status === "skipped";
}

function displaySourceName(source: string, value: Record<string, unknown>): string {
  return asText(value.source_label) || (source === "linkedin" ? "LinkedIn" : source === "indeed" ? "Indeed" : source);
}

function JobsStagePreview({
  taskType,
  resultPayload,
  taskId
}: {
  taskType: string;
  resultPayload: unknown;
  taskId?: string | null;
}): JSX.Element | null {
  const jobsSearchMode = (() => {
    const artifact = isRecord(resultPayload) ? resultPayload : {};
    const request = isRecord(artifact.request) ? artifact.request : {};
    return asText(artifact.search_mode) || asText(request.search_mode) || null;
  })();

  if (taskType === "jobs_collect_v1" || taskType === "jobs_normalize_v1") {
    const observability =
      taskType === "jobs_collect_v1"
        ? observabilityRecord(resultPayload, "collection_observability")
        : observabilityRecord(resultPayload, "normalization_observability");
    if (!observability) return null;

    const waterfall = isRecord(observability.waterfall) ? observability.waterfall : {};
    const operatorQuestions = isRecord(observability.operator_questions) ? observability.operator_questions : {};
    const runPreview = isRecord(observability.run_preview) ? observability.run_preview : {};
    const previewMessages = textArray(runPreview.messages);
    const bySourceRaw = isRecord(observability.by_source) ? observability.by_source : {};
    const rows = Object.entries(bySourceRaw)
      .filter(([, value]) => isRecord(value))
      .filter(([, value]) => !isInactiveSourceCoverageRow(value as Record<string, unknown>))
      .map(([source, value]) => ({ source, value: value as Record<string, unknown> }));

    const summaryCards =
      taskType === "jobs_collect_v1"
        ? [
            { label: "Raw Discovered", value: asNumber(waterfall.raw_jobs_discovered) ?? 0 },
            { label: "Query Count", value: asNumber(waterfall.queries_executed) ?? asNumber(waterfall.query_count_used) ?? 0 },
            { label: "Kept After Filter", value: asNumber(waterfall.kept_after_basic_filter) ?? 0 },
            { label: "Deduped", value: asNumber(waterfall.deduped_in_collection) ?? 0 }
          ]
        : [
            { label: "Raw Discovered", value: asNumber(waterfall.raw_jobs_discovered) ?? 0 },
            { label: "Normalized", value: asNumber(waterfall.normalized_count) ?? 0 },
            { label: "Unique After Dedupe", value: asNumber(waterfall.deduped_count) ?? 0 },
            { label: "Duplicates Collapsed", value: asNumber(waterfall.duplicates_collapsed) ?? 0 }
          ];

    const questionCards = [
      { label: "Did We Search Enough?", value: asText(operatorQuestions.searched_enough) || asText(operatorQuestions.did_we_search_enough) || "-" },
      { label: "Which Source Is Weak?", value: asText(operatorQuestions.which_source_is_weak) || "-" },
      { label: "Why Did Raw Count Collapse?", value: asText(operatorQuestions.why_raw_count_collapsed) || asText(operatorQuestions.why_did_raw_count_collapse) || "-" },
      { label: "Are We Missing Metadata?", value: asText(operatorQuestions.are_we_missing_metadata) || "-" }
    ];

    return (
      <div className="space-y-3">
        <div className="grid gap-2 sm:grid-cols-4">
          {summaryCards.map((card) => (
            <div key={card.label} className="rounded border border-border bg-card p-2 text-xs">
              <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">{card.label}</div>
              <div className="mt-1 text-sm font-semibold">{card.value}</div>
            </div>
          ))}
        </div>

        <div className="grid gap-2 sm:grid-cols-2">
          {questionCards.map((card) => (
            <div key={card.label} className="rounded border border-border bg-muted/20 p-3 text-xs">
              <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">{card.label}</div>
              <div className="mt-1">{card.value}</div>
            </div>
          ))}
        </div>

        {jobsSearchMode ? (
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Search Mode</div>
            <div className="mt-1">{jobsSearchMode.replace(/_/g, " ")}</div>
          </div>
        ) : null}

        {previewMessages.length > 0 ? (
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Preview Signals</div>
            <div className="mt-2 space-y-1">
              {previewMessages.map((message) => (
                <div key={message}>{message}</div>
              ))}
            </div>
          </div>
        ) : null}

        <div className="rounded border border-border bg-card p-3">
          <div className="mb-2 text-xs font-medium uppercase tracking-[0.06em] text-muted-foreground">By Source</div>
          {rows.length === 0 ? (
            <div className="text-xs text-muted-foreground">No source-level observability found.</div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Source</TableHead>
                  <TableHead>Health</TableHead>
                  <TableHead>Jobs</TableHead>
                  <TableHead>Pages</TableHead>
                  <TableHead>Signals</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map(({ source, value }) => {
                  const jobsSummary =
                    taskType === "jobs_collect_v1"
                      ? `${asNumber(value.raw_jobs_discovered) ?? 0} raw -> ${asNumber(value.kept_after_basic_filter) ?? 0} kept`
                      : `${asNumber(value.raw_jobs_discovered) ?? 0} raw -> ${asNumber(value.kept_after_basic_filter) ?? 0} kept -> ${asNumber(value.deduped_unique_groups) ?? 0} unique`;
                  const pagesSummary = `${asNumber(value.pages_attempted) ?? asNumber(value.pages_fetched) ?? 0} attempted`;
                  const gaps =
                    asText(value.weakness_summary) ||
                    [
                      `company ${formatPercent(value.missing_company_rate)}`,
                      `post date ${formatPercent(value.missing_posted_at_rate)}`,
                      `link ${formatPercent(value.missing_source_url_rate)}`,
                      `location ${formatPercent(value.missing_location_rate)}`
                    ].join(", ");
                  const healthStatus = asText(value.status) || "unknown";
                  const usableJobs = asNumber(value.jobs_kept) ?? asNumber(value.final_raw_jobs) ?? 0;
                  const signals = [
                    asBoolean(value.under_target) ? "under target" : null,
                    asBoolean(value.suspected_blocking)
                      ? `suspected blocking${asText(value.suspected_blocking_reason) ? ` (${asText(value.suspected_blocking_reason)})` : ""}`
                      : null,
                    gaps
                  ]
                    .filter(Boolean)
                    .join(" · ");

                  return (
                    <TableRow key={source}>
                      <TableCell>
                        <div className="space-y-0.5">
                          <div className="font-medium">{displaySourceName(source, value)}</div>
                          {usableJobs <= 0 ? (
                            <div className="text-[11px] text-muted-foreground">No usable jobs collected</div>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell className="text-xs">
                        <StatusBadge status={healthStatus} />
                      </TableCell>
                      <TableCell className="text-xs">{jobsSummary}</TableCell>
                      <TableCell className="text-xs">{pagesSummary}</TableCell>
                      <TableCell className="text-xs">{signals}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </div>

        {renderJobsPreviewActions(jobsStageActions(taskType, taskId))}
      </div>
    );
  }

  if (taskType === "jobs_rank_v1") {
    const artifact = isRecord(resultPayload) ? resultPayload : {};
    const pipelineCounts = isRecord(artifact.pipeline_counts) ? artifact.pipeline_counts : {};
    const debug = isRecord(artifact.model_usage) ? artifact.model_usage : {};
    const rankPolicy = isRecord(artifact.rank_policy) ? artifact.rank_policy : {};
    const rankedJobs = jobsPreviewRows(resultPayload);
    const llmMeta = isRecord(artifact.jobs_scored_artifact) && isRecord((artifact.jobs_scored_artifact as Record<string, unknown>).llm_meta)
      ? ((artifact.jobs_scored_artifact as Record<string, unknown>).llm_meta as Record<string, unknown>)
      : {};
    const cards = [
      { label: "Input Jobs", value: asNumber(artifact.input_jobs_count) ?? 0 },
      { label: "Filtered Jobs", value: asNumber(artifact.filtered_jobs_count) ?? 0 },
      { label: "Scored Jobs", value: asNumber(pipelineCounts.scored_count) ?? rankedJobs.length },
      { label: "LLM Attempts", value: asNumber(llmMeta.attempts_total) ?? 0 }
    ];
    const llmStatus = [
      `runtime ${asText(debug.llm_runtime_enabled) || "false"}`,
      `fallback ${asText(llmMeta.fallback_used) || "false"}`
    ].join(" · ");

    return (
      <div className="space-y-3">
        <div className="grid gap-2 sm:grid-cols-4">
          {cards.map((card) => (
            <div key={card.label} className="rounded border border-border bg-card p-2 text-xs">
              <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">{card.label}</div>
              <div className="mt-1 text-sm font-semibold">{card.value}</div>
            </div>
          ))}
        </div>
        <div className="grid gap-2 sm:grid-cols-2">
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">LLM Status</div>
            <div className="mt-1">{llmStatus}</div>
          </div>
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Prompt Version</div>
            <div className="mt-1">{asText(debug.prompt_version) || asText(rankPolicy.prompt_version) || "-"}</div>
          </div>
        </div>
        {jobsSearchMode ? (
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Search Mode</div>
            <div className="mt-1">{jobsSearchMode.replace(/_/g, " ")}</div>
          </div>
        ) : null}
        {rankedJobs.length > 0 ? (
          <div className="space-y-2 rounded border border-border bg-card p-3">
            {rankedJobs.slice(0, 5).map((row, idx) => (
              <div key={`${asText(row.title) || "job"}-${idx}`} className="rounded border border-border/80 bg-muted/20 px-2 py-2 text-xs">
                <div className="font-medium">{asText(row.title) || `Job #${idx + 1}`}</div>
                <div className="mt-1 text-muted-foreground">
                  {(asText(row.company) || "unknown")} · score {asNumber(row.overall_score) ?? asNumber(row.score) ?? "-"} · metadata {asNumber(row.metadata_quality_score) ?? "-"}
                </div>
              </div>
            ))}
          </div>
        ) : null}
        {renderJobsPreviewActions(jobsStageActions(taskType, taskId))}
      </div>
    );
  }

  if (taskType === "jobs_shortlist_v1") {
    const artifact = isRecord(resultPayload) ? resultPayload : {};
    const summary = isRecord(artifact.shortlist_summary_metadata) ? artifact.shortlist_summary_metadata : {};
    const pipelineCounts = isRecord(artifact.pipeline_counts) ? artifact.pipeline_counts : {};
    const history = isRecord(artifact.history_observability) ? artifact.history_observability : {};
    const shortlist = jobsPreviewRows(resultPayload);
    const cards = [
      { label: "Shortlisted", value: asNumber(artifact.shortlist_count) ?? shortlist.length },
      { label: "Scored Input", value: asNumber(summary.input_scored_count) ?? asNumber(pipelineCounts.scored_count) ?? 0 },
      { label: "Newly Discovered", value: asNumber(history.selected_newly_discovered_count) ?? 0 },
      { label: "Resurfaced", value: asNumber(history.selected_resurfaced_count) ?? 0 }
    ];
    const historyBits = [
      `previously shortlisted ${asNumber(history.selected_previously_shortlisted_count) ?? 0}`,
      `previously notified ${asNumber(history.selected_previously_notified_count) ?? 0}`,
      `cooldown suppressed ${asNumber(history.cooldown_suppressed_count) ?? 0}`
    ].join(" · ");

    return (
      <div className="space-y-3">
        <div className="grid gap-2 sm:grid-cols-4">
          {cards.map((card) => (
            <div key={card.label} className="rounded border border-border bg-card p-2 text-xs">
              <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">{card.label}</div>
              <div className="mt-1 text-sm font-semibold">{card.value}</div>
            </div>
          ))}
        </div>
        <div className="rounded border border-border bg-muted/20 p-3 text-xs">
          <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">History / Repeat Behavior</div>
          <div className="mt-1">{historyBits}</div>
        </div>
        {jobsSearchMode ? (
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Search Mode</div>
            <div className="mt-1">{jobsSearchMode.replace(/_/g, " ")}</div>
          </div>
        ) : null}
        {shortlist.length > 0 ? (
          <div className="space-y-2 rounded border border-border bg-card p-3">
            {shortlist.slice(0, 5).map((row, idx) => (
              <div key={`${asText(row.title) || "job"}-${idx}`} className="rounded border border-border/80 bg-muted/20 px-2 py-2 text-xs">
                <div className="font-medium">{asText(row.title) || `Job #${idx + 1}`}</div>
                <div className="mt-1 text-muted-foreground">
                  {(asText(row.company) || "unknown")} · {asText(row.source) || "unknown source"}
                </div>
                <div className="mt-1 text-muted-foreground">
                  {row.newly_discovered === true ? "new" : row.resurfaced_from_prior_runs === true ? "resurfaced" : "prior run state unknown"}
                  {row.previously_shortlisted === true ? " · previously shortlisted" : ""}
                  {row.previously_notified === true ? " · previously notified" : ""}
                </div>
              </div>
            ))}
          </div>
        ) : null}
        {renderJobsPreviewActions(jobsStageActions(taskType, taskId))}
      </div>
    );
  }

  if (taskType === "jobs_digest_v2") {
    const artifact = isRecord(resultPayload) ? resultPayload : {};
    const pipelineCounts = isRecord(artifact.pipeline_counts) ? artifact.pipeline_counts : {};
    const notifyDecision = isRecord(artifact.notify_decision) ? artifact.notify_decision : {};
    const llmMeta = isRecord(artifact.model_usage) ? artifact.model_usage as Record<string, unknown> : {};
    const digestJobs = jobsPreviewRows(resultPayload);
    const cards = [
      { label: "Shortlist Count", value: asNumber(pipelineCounts.shortlisted_count) ?? digestJobs.length },
      { label: "Notify", value: asText(notifyDecision.should_notify) || "false" },
      { label: "LLM Attempts", value: asNumber(llmMeta.attempts) ?? 0 },
      { label: "Generation Mode", value: asText(artifact.generation_mode) || "-" }
    ];

    return (
      <div className="space-y-3">
        <div className="grid gap-2 sm:grid-cols-4">
          {cards.map((card) => (
            <div key={card.label} className="rounded border border-border bg-card p-2 text-xs">
              <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">{card.label}</div>
              <div className="mt-1 text-sm font-semibold">{card.value}</div>
            </div>
          ))}
        </div>
        <div className="grid gap-2 sm:grid-cols-2">
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Notify Decision</div>
            <div className="mt-1">{asText(notifyDecision.reason) || "-"}</div>
          </div>
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Fallback Status</div>
            <div className="mt-1">{`fallback ${asText(llmMeta.fallback_used) || "false"} · strict ${asText(llmMeta.strict_failure) || "false"}`}</div>
          </div>
        </div>
        {jobsSearchMode ? (
          <div className="rounded border border-border bg-muted/20 p-3 text-xs">
            <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Search Mode</div>
            <div className="mt-1">{jobsSearchMode.replace(/_/g, " ")}</div>
          </div>
        ) : null}
        {(asText(artifact.summary) || asText(artifact.notification_excerpt)) ? (
          <div className="rounded border border-border bg-card p-3 text-xs leading-relaxed">
            {asText(artifact.summary) || asText(artifact.notification_excerpt)}
          </div>
        ) : null}
        {digestJobs.length > 0 ? (
          <div className="space-y-2 rounded border border-border bg-card p-3">
            {digestJobs.slice(0, 4).map((row, idx) => {
              const url = asText(row.source_url) || asText(row.url);
              return (
                <div key={`${asText(row.title) || "job"}-${idx}`} className="rounded border border-border/80 bg-muted/20 px-2 py-2 text-xs">
                  <div className="font-medium">{asText(row.title) || `Job #${idx + 1}`}</div>
                  <div className="mt-1 text-muted-foreground">
                    {(asText(row.company) || "unknown")} · {asText(row.source) || "unknown source"}{asText(row.posted_display) || asText(row.posted) ? ` · ${asText(row.posted_display) || asText(row.posted)}` : ""}
                  </div>
                  {url ? (
                    <a className="mt-1 inline-block break-all text-primary underline" href={url} target="_blank" rel="noreferrer">
                      {url}
                    </a>
                  ) : null}
                </div>
              );
            })}
          </div>
        ) : null}
        {renderJobsPreviewActions(jobsStageActions(taskType, taskId))}
      </div>
    );
  }

  return null;
}

function JobsPreviewBody({
  taskType,
  resultPayload,
  taskId
}: {
  taskType: string | undefined;
  resultPayload: unknown;
  taskId?: string | null;
}): JSX.Element {
  if (taskType === "jobs_collect_v1" || taskType === "jobs_normalize_v1" || taskType === "jobs_rank_v1" || taskType === "jobs_shortlist_v1" || taskType === "jobs_digest_v2") {
    const stagePreview = JobsStagePreview({ taskType, resultPayload, taskId });
    if (stagePreview) return stagePreview;
  }

  const jobs = jobsPreviewRows(resultPayload);
  if (jobs.length === 0) {
    return <PreviewFallback payload={resultPayload} />;
  }

  return (
    <div className="space-y-3">
      <div className="rounded border border-border bg-card p-2 text-xs">
        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Jobs in payload</div>
        <div className="mt-1 text-sm font-semibold">{jobs.length}</div>
      </div>

      <div className="space-y-2 rounded border border-border bg-card p-3">
        {jobs.slice(0, 6).map((row, idx) => {
          const title = asText(row.title) || asText(row.job_title) || asText(row.role) || `Job #${idx + 1}`;
          const company = asText(row.company) || asText(row.employer) || "unknown";
          const location = asText(row.location) || asText(row.city) || "-";
          const compensation = asText(row.salary) || asText(row.salary_range) || asText(row.compensation) || "-";
          const url = asText(row.url) || asText(row.link);

          return (
            <div key={`${title}-${idx}`} className="rounded border border-border/80 bg-muted/20 px-2 py-2 text-xs">
              <div className="font-medium">{title}</div>
              <div className="mt-1 text-muted-foreground">{company} · {location} · {compensation}</div>
              {url ? (
                <a className="mt-1 inline-block break-all text-primary underline" href={url} target="_blank" rel="noreferrer">
                  {url}
                </a>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ResultPreview({
  taskType,
  resultPayload,
  taskPayload,
  taskId
}: {
  taskType: string | undefined;
  resultPayload: unknown;
  taskPayload: unknown | null;
  taskId?: string | null;
}): JSX.Element {
  if (!taskType) return <PreviewFallback payload={resultPayload} />;

  if (taskType === "notify_v1") {
    return <NotifyPreview resultPayload={resultPayload} taskPayload={taskPayload} />;
  }
  if (taskType === "deals_scan_v1") {
    return <DealsPreview resultPayload={resultPayload} />;
  }
  if (taskType.startsWith("jobs_")) {
    return <JobsPreviewBody taskType={taskType} resultPayload={resultPayload} taskId={taskId} />;
  }

  return <PreviewFallback payload={resultPayload} />;
}

export function RunsPage(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedTaskIdFromQuery = searchParams.get("task_id");
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(selectedTaskIdFromQuery);
  const statusFilter = searchParams.get("status") || "all";
  const taskTypeFilterFromQuery = searchParams.get("task_type") || "";
  const [taskTypeFilter, setTaskTypeFilter] = useState(taskTypeFilterFromQuery);

  useEffect(() => {
    setTaskTypeFilter(taskTypeFilterFromQuery);
  }, [taskTypeFilterFromQuery]);

  useEffect(() => {
    setSelectedTaskId(selectedTaskIdFromQuery);
  }, [selectedTaskIdFromQuery]);

  const updateQueryFilters = (nextStatus: string, nextTaskType: string, replace = false, nextTaskId = selectedTaskId): void => {
    const next = new URLSearchParams();
    if (nextStatus !== "all") next.set("status", nextStatus);
    if (nextTaskType.trim()) next.set("task_type", nextTaskType.trim());
    if (nextTaskId) next.set("task_id", nextTaskId);
    setSearchParams(next, { replace });
  };

  const selectTask = (nextTaskId: string | null, replace = false): void => {
    setSelectedTaskId(nextTaskId);
    updateQueryFilters(statusFilter, taskTypeFilter, replace, nextTaskId);
  };

  const tasksQuery = useTasks(120);
  const runsQuery = useRuns(400);
  const selectedTaskQuery = useTask(selectedTaskId);
  const taskRunsQuery = useTaskRuns(selectedTaskId, 40);
  const taskResultQuery = useTaskResult(selectedTaskId);

  const filteredTasks = useMemo(() => {
    return (tasksQuery.data || []).filter((task) => {
      const matchesStatus = statusFilter === "all" ? true : task.status === statusFilter;
      const matchesType = taskTypeFilter.trim() ? task.task_type.toLowerCase().includes(taskTypeFilter.toLowerCase()) : true;
      return matchesStatus && matchesType;
    });
  }, [tasksQuery.data, statusFilter, taskTypeFilter]);

  const runAttemptsByTaskId = useMemo(() => {
    const map: Record<string, number> = {};
    (runsQuery.data || []).forEach((run) => {
      map[run.task_id] = (map[run.task_id] || 0) + 1;
    });
    return map;
  }, [runsQuery.data]);

  const selectedTask = selectedTaskQuery.data;
  const selectedRuns = useMemo(() => taskRunsQuery.data || [], [taskRunsQuery.data]);
  const selectedResult = taskResultQuery.data || null;
  const selectedResultPayload = resolveResultPayload(selectedResult);
  const selectedTaskPayload = parseTaskPayload(selectedTask);
  const selectedAttemptCount = selectedRuns.length;
  const selectedFailureMode = selectedTask ? taskFailureMode(selectedTask, selectedAttemptCount) : null;

  const artifactRows = useMemo(() => {
    return [
      {
        name: "Task Payload",
        type: "payload_json",
        status: selectedTask ? "available" : "missing",
        capturedAt: selectedTask?.created_at,
        notes: selectedTask ? "Input payload attached to task" : "Task not loaded"
      },
      {
        name: "Execution Attempts",
        type: "run_history",
        status: selectedRuns.length > 0 ? "available" : "missing",
        capturedAt: selectedRuns.length > 0 ? selectedRuns[selectedRuns.length - 1].created_at : null,
        notes: `${selectedRuns.length} attempt${selectedRuns.length === 1 ? "" : "s"} recorded`
      },
      {
        name: "Latest Result",
        type: selectedResult?.artifact_type || "result.json",
        status: selectedResult ? "available" : "missing",
        capturedAt: selectedResult?.created_at,
        notes: selectedResult ? "Result artifact fetched" : "No result artifact yet"
      }
    ];
  }, [selectedResult, selectedRuns, selectedTask]);

  const pageError = [tasksQuery.error, runsQuery.error].find(Boolean);
  const detailsError = [selectedTaskQuery.error, taskRunsQuery.error, taskResultQuery.error].find(Boolean);

  const retryAll = (): void => {
    void Promise.all([tasksQuery.refetch(), runsQuery.refetch()]);
    if (selectedTaskId) {
      void Promise.all([selectedTaskQuery.refetch(), taskRunsQuery.refetch(), taskResultQuery.refetch()]);
    }
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Runs"
        subtitle="Operator debugging surface for task execution timeline, attempts, artifacts, and previews."
        actions={
          <div className="flex flex-wrap gap-2">
            <Button variant={statusFilter === "all" ? "default" : "secondary"} size="sm" onClick={() => updateQueryFilters("all", taskTypeFilter)}>All</Button>
            <Button variant={statusFilter === "failed" ? "default" : "secondary"} size="sm" onClick={() => updateQueryFilters("failed", taskTypeFilter)}>Failed</Button>
            <Button variant={statusFilter === "running" ? "default" : "secondary"} size="sm" onClick={() => updateQueryFilters("running", taskTypeFilter)}>Running</Button>
            <Button variant={statusFilter === "success" ? "default" : "secondary"} size="sm" onClick={() => updateQueryFilters("success", taskTypeFilter)}>Success</Button>
          </div>
        }
      />
      {pageError ? <ErrorPanel title="Runs failed to load" message={errorMessage(pageError)} onAction={retryAll} /> : null}

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_480px]">
        <div className="space-y-4">
          <Card>
            <CardContent className="p-4">
              <SectionHeader title="Execution Filters" subtitle="Filter by task type and inspect failure-heavy queues quickly." />
              <div className="grid gap-3 md:grid-cols-[1fr_auto_auto] md:items-end">
                <Input
                  value={taskTypeFilter}
                  onChange={(e) => {
                    const nextValue = e.target.value;
                    setTaskTypeFilter(nextValue);
                    updateQueryFilters(statusFilter, nextValue, true);
                  }}
                  placeholder="Filter by task type"
                />
                <div className="text-xs text-muted-foreground">Rows: {filteredTasks.length}</div>
                <Button size="sm" variant="outline" onClick={() => selectTask(null)}>Clear Selection</Button>
              </div>
            </CardContent>
          </Card>

          <DataTableWrapper
            title="Executions"
            subtitle="Select a row to open summary, attempts, artifacts, and preview in the details panel."
            loading={tasksQuery.isLoading}
            error={tasksQuery.error ? errorMessage(tasksQuery.error) : null}
            onRetry={() => void tasksQuery.refetch()}
            isEmpty={!tasksQuery.isLoading && filteredTasks.length === 0}
            emptyTitle="No runs match your filter"
            emptyDescription="Adjust filters or create a new workflow run to populate this view."
          >
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Task</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Failure</TableHead>
                  <TableHead>Attempts</TableHead>
                  <TableHead>Cost</TableHead>
                  <TableHead>Updated</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredTasks.map((task) => {
                  const attemptsUsed = runAttemptsByTaskId[task.id] || 0;
                  const failureMode = taskFailureMode(task, attemptsUsed);
                  const isSelected = selectedTaskId === task.id;

                  return (
                    <TableRow
                      key={task.id}
                      className={isSelected ? "cursor-pointer bg-primary/10" : "cursor-pointer"}
                      onClick={() => selectTask(task.id)}
                    >
                      <TableCell>
                        <div className="space-y-0.5">
                          <div className="font-medium">{task.task_type}</div>
                          <div className="font-mono text-[10px] text-muted-foreground">{task.id}</div>
                        </div>
                      </TableCell>
                      <TableCell><StatusBadge status={task.status} /></TableCell>
                      <TableCell>
                        {task.status === "blocked_budget" ? (
                          <StatusBadge status="blocked_budget" />
                        ) : failureMode ? (
                          <StatusBadge status={failureMode} />
                        ) : (
                          <span className="text-xs text-muted-foreground">-</span>
                        )}
                      </TableCell>
                      <TableCell>
                        <div className="text-xs">{attemptsUsed} / {task.max_attempts}</div>
                      </TableCell>
                      <TableCell>{formatCost(task.cost_usd)}</TableCell>
                      <TableCell>{timestampLabel(task.updated_at)}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </DataTableWrapper>
        </div>

        <DetailsSurface
          title="Run Details"
          open={Boolean(selectedTaskId)}
          onClose={() => selectTask(null)}
          empty={
            <EmptyState
              title="No run selected"
              description="Select an execution row to inspect summary, attempts, artifacts, result preview, and raw JSON."
            />
          }
        >
          {selectedTaskId ? (
            <div className="space-y-4">
              {detailsError ? <ErrorPanel title="Run detail request failed" message={errorMessage(detailsError)} onAction={retryAll} /> : null}

              <section>
                <SectionHeader title="Summary" subtitle="Task status, failure mode, attempts, and key execution metadata." />
                {selectedTask ? (
                  <div className="space-y-3 rounded border border-border bg-card p-3 text-xs">
                    <div className="grid gap-2 sm:grid-cols-2">
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Task Type</div>
                        <div className="mt-1">{selectedTask.task_type}</div>
                      </div>
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Task ID</div>
                        <div className="mt-1 font-mono break-all">{selectedTask.id}</div>
                      </div>
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Status</div>
                        <div className="mt-1"><StatusBadge status={selectedTask.status} /></div>
                      </div>
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Failure Mode</div>
                        <div className="mt-1">
                          {selectedTask.status === "blocked_budget" ? (
                            <StatusBadge status="blocked_budget" />
                          ) : selectedFailureMode ? (
                            <StatusBadge status={selectedFailureMode} />
                          ) : (
                            <span className="text-muted-foreground">none</span>
                          )}
                        </div>
                      </div>
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Attempts</div>
                        <div className="mt-1">{selectedAttemptCount} / {selectedTask.max_attempts}</div>
                      </div>
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Cost</div>
                        <div className="mt-1">{formatCost(selectedTask.cost_usd)}</div>
                      </div>
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Created</div>
                        <div className="mt-1">{timestampLabel(selectedTask.created_at)}</div>
                      </div>
                      <div>
                        <div className="font-medium uppercase tracking-[0.06em] text-muted-foreground">Updated</div>
                        <div className="mt-1">{timestampLabel(selectedTask.updated_at)}</div>
                      </div>
                    </div>
                    {describeDiagnostics(selectedTask) ? (
                      <div className="rounded border border-destructive/35 bg-destructive/10 p-2 text-destructive">
                        {describeDiagnostics(selectedTask)}
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <div className="text-sm text-muted-foreground">Loading summary…</div>
                )}
              </section>

              <section>
                <SectionHeader title="Attempts" subtitle="Per-attempt status with retry/permanent distinction and timestamps." />
                {taskRunsQuery.isLoading ? (
                  <div className="text-sm text-muted-foreground">Loading attempts…</div>
                ) : selectedRuns.length === 0 ? (
                  <EmptyState title="No attempts recorded" description="This task has not started execution yet." />
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>#</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead>Failure</TableHead>
                        <TableHead>Started</TableHead>
                        <TableHead>Ended</TableHead>
                        <TableHead>Duration</TableHead>
                        <TableHead>Cost</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {selectedRuns.map((run) => {
                        const mode = runFailureMode(run, selectedTask);
                        return (
                          <TableRow key={run.id}>
                            <TableCell className="font-mono text-[11px]">{run.attempt}</TableCell>
                            <TableCell><StatusBadge status={run.status} /></TableCell>
                            <TableCell>{mode ? <StatusBadge status={mode} /> : <span className="text-xs text-muted-foreground">-</span>}</TableCell>
                            <TableCell>{timestampLabel(run.started_at)}</TableCell>
                            <TableCell>{timestampLabel(run.ended_at)}</TableCell>
                            <TableCell>{formatDurationMs(run.wall_time_ms)}</TableCell>
                            <TableCell>{formatCost(run.cost_usd)}</TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                )}
              </section>

              <section>
                <SectionHeader title="Artifacts" subtitle="Availability and freshness of payload, run history, and latest result artifact." />
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Name</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Captured</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {artifactRows.map((row) => (
                      <TableRow key={row.name}>
                        <TableCell>
                          <div className="space-y-0.5">
                            <div className="font-medium">{row.name}</div>
                            <div className="text-[11px] text-muted-foreground">{row.notes}</div>
                          </div>
                        </TableCell>
                        <TableCell className="font-mono text-[11px]">{row.type}</TableCell>
                        <TableCell><StatusBadge status={row.status} /></TableCell>
                        <TableCell>{timestampLabel(row.capturedAt)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </section>

              <section>
                <SectionHeader title="Result Preview" subtitle="Task-type-aware preview for operator scanability. Raw payload remains available below." />
                <ResultPreview taskType={selectedTask?.task_type} resultPayload={selectedResultPayload} taskPayload={selectedTaskPayload} taskId={selectedTask?.id || null} />
              </section>

              <section>
                <SectionHeader title="Raw JSON" subtitle="Collapsed by default. Expand for full debug context." />
                <details className="rounded border border-border bg-muted/20 p-2">
                  <summary className="cursor-pointer text-xs font-medium">Show task / attempts / result JSON</summary>
                  <div className="mt-3 space-y-3">
                    <div>
                      <div className="mb-1 text-xs font-semibold uppercase tracking-[0.06em] text-muted-foreground">Task</div>
                      <JsonViewer value={selectedTask || {}} />
                    </div>
                    <div>
                      <div className="mb-1 text-xs font-semibold uppercase tracking-[0.06em] text-muted-foreground">Attempts</div>
                      <JsonViewer value={selectedRuns} />
                    </div>
                    <div>
                      <div className="mb-1 text-xs font-semibold uppercase tracking-[0.06em] text-muted-foreground">Result</div>
                      <JsonViewer value={selectedResult || {}} />
                    </div>
                  </div>
                </details>
              </section>
            </div>
          ) : null}
        </DetailsSurface>
      </div>
    </div>
  );
}
