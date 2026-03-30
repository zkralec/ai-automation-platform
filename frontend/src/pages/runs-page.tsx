import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
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

type RunFailureMode = "retryable" | "permanent" | null;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asText(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
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

function JobsPreview({ resultPayload }: { resultPayload: unknown }): JSX.Element {
  const jobs = pickRecordArray(resultPayload, [
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
    "top_jobs"
  ]);
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

function ResultPreview({ taskType, resultPayload, taskPayload }: { taskType: string | undefined; resultPayload: unknown; taskPayload: unknown | null }): JSX.Element {
  if (!taskType) return <PreviewFallback payload={resultPayload} />;

  if (taskType === "notify_v1") {
    return <NotifyPreview resultPayload={resultPayload} taskPayload={taskPayload} />;
  }
  if (taskType === "deals_scan_v1") {
    return <DealsPreview resultPayload={resultPayload} />;
  }
  if (taskType.startsWith("jobs_")) {
    return <JobsPreview resultPayload={resultPayload} />;
  }

  return <PreviewFallback payload={resultPayload} />;
}

export function RunsPage(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const statusFilter = searchParams.get("status") || "all";
  const taskTypeFilterFromQuery = searchParams.get("task_type") || "";
  const [taskTypeFilter, setTaskTypeFilter] = useState(taskTypeFilterFromQuery);

  useEffect(() => {
    setTaskTypeFilter(taskTypeFilterFromQuery);
  }, [taskTypeFilterFromQuery]);

  const updateQueryFilters = (nextStatus: string, nextTaskType: string, replace = false): void => {
    const next = new URLSearchParams();
    if (nextStatus !== "all") next.set("status", nextStatus);
    if (nextTaskType.trim()) next.set("task_type", nextTaskType.trim());
    setSearchParams(next, { replace });
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
                <Button size="sm" variant="outline" onClick={() => setSelectedTaskId(null)}>Clear Selection</Button>
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
                      onClick={() => setSelectedTaskId(task.id)}
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
          onClose={() => setSelectedTaskId(null)}
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
                    {selectedTask.error ? (
                      <div className="rounded border border-destructive/35 bg-destructive/10 p-2 text-destructive">
                        {selectedTask.error}
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
                <ResultPreview taskType={selectedTask?.task_type} resultPayload={selectedResultPayload} taskPayload={selectedTaskPayload} />
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
