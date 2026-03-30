import { useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Play } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import { DataTableWrapper } from "@/components/common/data-table-wrapper";
import { EmptyState } from "@/components/common/empty-state";
import { ErrorPanel } from "@/components/common/error-panel";
import { PageHeader } from "@/components/common/page-header";
import { SectionHeader } from "@/components/common/section-header";
import { StatusBadge } from "@/components/common/status-badge";
import { WorkflowCard } from "@/components/common/workflow-card";
import {
  usePatchPlannerConfigMutation,
  usePlannerConfig,
  usePlannerStatus,
  useResetPlannerConfigMutation,
  useRunPlannerOnceMutation,
  useSaveJobsPresetMutation,
  useSaveRtxPresetMutation
} from "@/features/planner/queries";
import { useCreateTaskMutation } from "@/features/tasks/queries";
import type { Watcher, WatcherPatchInput } from "@/features/watchers/api";
import {
  useCreateWatcherMutation,
  useDeleteWatcherMutation,
  usePatchWatcherMutation,
  useWatchers
} from "@/features/watchers/queries";
import { errorMessage } from "@/lib/utils/errors";
import { formatIso } from "@/lib/utils/format";

const PRESET_RTX_WATCHER_ID = "preset-rtx5090-deals-scan";
const PRESET_JOBS_WATCHER_ID = "preset-jobs-digest-scan";
const NOTIFY_ALLOWED_SEVERITIES = new Set(["info", "warn", "urgent"]);
const DEFAULT_NOTIFY_WORKFLOW_PAYLOAD: Record<string, unknown> = {
  channels: ["discord"],
  message: "Mission Control daily ops summary",
  source_task_type: "ops_report_v1",
  dedupe_key: "watcher-daily-ops",
  severity: "info",
  include_header: false,
  include_metadata: false
};

function normalizeNotifyWorkflowPayload(rawPayloadJson: string | null | undefined): string {
  const fallbackMessage = String(DEFAULT_NOTIFY_WORKFLOW_PAYLOAD.message);
  const fallbackSourceTaskType = String(DEFAULT_NOTIFY_WORKFLOW_PAYLOAD.source_task_type);
  const fallbackDedupe = String(DEFAULT_NOTIFY_WORKFLOW_PAYLOAD.dedupe_key);

  let payload: Record<string, unknown> = {};
  if (rawPayloadJson) {
    try {
      const parsed = JSON.parse(rawPayloadJson);
      if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
        payload = parsed as Record<string, unknown>;
      }
    } catch {
      payload = {};
    }
  }

  const rawChannels = Array.isArray(payload.channels)
    ? payload.channels
    : typeof payload.channel === "string"
      ? [payload.channel]
      : [];
  const channels = rawChannels
    .map((row) => (typeof row === "string" ? row.trim().toLowerCase() : ""))
    .filter((row) => row === "discord");
  const message = typeof payload.message === "string" && payload.message.trim() ? payload.message.trim() : fallbackMessage;
  const sourceTaskType =
    typeof payload.source_task_type === "string" && payload.source_task_type.trim()
      ? payload.source_task_type.trim()
      : fallbackSourceTaskType;
  const severity = typeof payload.severity === "string" && NOTIFY_ALLOWED_SEVERITIES.has(payload.severity) ? payload.severity : "info";
  const dedupeKey = typeof payload.dedupe_key === "string" && payload.dedupe_key.trim() ? payload.dedupe_key.trim() : fallbackDedupe;

  const normalized: Record<string, unknown> = {
    channels: channels.length > 0 ? channels : ["discord"],
    message,
    source_task_type: sourceTaskType,
    dedupe_key: dedupeKey,
    severity
  };

  if (typeof payload.dedupe_ttl_seconds === "number" && Number.isInteger(payload.dedupe_ttl_seconds) && payload.dedupe_ttl_seconds >= 1) {
    normalized.dedupe_ttl_seconds = payload.dedupe_ttl_seconds;
  }
  if (typeof payload.include_header === "boolean") {
    normalized.include_header = payload.include_header;
  }
  if (typeof payload.include_metadata === "boolean") {
    normalized.include_metadata = payload.include_metadata;
  }
  if (typeof payload.metadata === "object" && payload.metadata !== null && !Array.isArray(payload.metadata)) {
    normalized.metadata = payload.metadata;
  }

  return JSON.stringify(normalized);
}

const JOB_SOURCE_OPTIONS = ["linkedin", "indeed", "glassdoor", "handshake"] as const;
type JobSourceOption = (typeof JOB_SOURCE_OPTIONS)[number];

const JOB_WORK_MODE_OPTIONS = ["remote", "hybrid", "onsite"] as const;
type JobWorkModeOption = (typeof JOB_WORK_MODE_OPTIONS)[number];

const JOB_EXPERIENCE_LEVEL_OPTIONS = ["", "internship", "entry", "mid", "senior"] as const;
const JOB_FRESHNESS_PREFERENCE_OPTIONS = ["off", "prefer_recent", "strong_prefer_recent"] as const;

type JobsWatcherFormState = {
  desiredTitlesText: string;
  keywordsText: string;
  excludedKeywordsText: string;
  preferredLocationsText: string;
  remotePreference: Record<JobWorkModeOption, boolean>;
  minimumSalaryText: string;
  experienceLevel: string;
  enabledSources: Record<JobSourceOption, boolean>;
  resultLimitPerSourceText: string;
  shortlistCountText: string;
  freshnessPreference: string;
};

const DEFAULT_JOBS_WATCHER_FORM_STATE: JobsWatcherFormState = {
  desiredTitlesText: "software engineer",
  keywordsText: "",
  excludedKeywordsText: "",
  preferredLocationsText: "United States",
  remotePreference: { remote: true, hybrid: true, onsite: false },
  minimumSalaryText: "",
  experienceLevel: "",
  enabledSources: { linkedin: true, indeed: true, glassdoor: true, handshake: true },
  resultLimitPerSourceText: "25",
  shortlistCountText: "5",
  freshnessPreference: "off"
};

const PRIMARY_WATCHERS: Array<{
  id: "deals" | "jobs" | "notify";
  watcherId: string;
  name: string;
  taskType: string;
  description: string;
  defaultInterval: number;
  payload: Record<string, unknown>;
}> = [
  {
    id: "deals",
    watcherId: PRESET_RTX_WATCHER_ID,
    name: "RTX 5090 Deals",
    taskType: "deals_scan_v1",
    description: "Monitors RTX 5090 pricing and collector activity against unicorn thresholds.",
    defaultInterval: 300,
    payload: {
      source: "watcher-console",
      collectors_enabled: true,
      unicorn_gpu_5090_max_price: 2000,
      unicorn_pc_5090_max_price: 4000
    }
  },
  {
    id: "jobs",
    watcherId: PRESET_JOBS_WATCHER_ID,
    name: "Jobs Pipeline v2",
    taskType: "jobs_collect_v1",
    description: "Collects, normalizes, ranks, shortlists, and digests target jobs using profile context.",
    defaultInterval: 300,
    payload: {
      request: {
        collectors_enabled: true,
        profile_mode: "resume_profile",
        query: "software engineer",
        location: "United States",
        sources: ["linkedin", "indeed", "glassdoor", "handshake"],
        notify_on_empty: false
      }
    }
  },
  {
    id: "notify",
    watcherId: "watcher-daily-ops-notify",
    name: "Daily Ops Notifications",
    taskType: "notify_v1",
    description: "Monitors and emits daily operator notifications for baseline operational visibility.",
    defaultInterval: 600,
    payload: DEFAULT_NOTIFY_WORKFLOW_PAYLOAD
  }
];

type PrimaryWatcherId = (typeof PRIMARY_WATCHERS)[number]["id"];
type PrimaryIntervals = Record<PrimaryWatcherId, number>;
const DEFAULT_PRIMARY_INTERVALS: PrimaryIntervals = {
  deals: 300,
  jobs: 300,
  notify: 600
};
const FAILED_TASK_STATUSES = new Set(["failed", "failed_permanent", "blocked_budget"]);
const SUCCESS_TASK_STATUSES = new Set(["success"]);
const NOTIFY_TASK_TYPE = "notify_v1";

type WorkflowInsight = {
  id: string;
  workflowName: string;
  taskType: string;
  description: string;
  stateLabel: string;
  effectiveIntervalLabel: string;
  lastRunTimeLabel: string;
  lastRunStatusRaw: string | null;
  lastRunOutcomeLabel: string;
  lastResultSummary: string;
  nextLikelyAction: string;
  notificationBehaviorLabel: string | null;
  watcherId: string | null;
};

function summarizeText(value: string | null | undefined, max = 170): string {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text) return "";
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}

function parsePayloadObject(payloadJson: string | null | undefined): Record<string, unknown> {
  if (!payloadJson) return {};
  try {
    const parsed = JSON.parse(payloadJson);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch {
    return {};
  }
  return {};
}

function parseTextList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => (typeof item === "string" ? item.trim() : ""))
    .filter(Boolean);
}

function withNotifyTestDedupeBypass(taskType: string, payload: Record<string, unknown>): Record<string, unknown> {
  if (taskType !== NOTIFY_TASK_TYPE) return payload;
  return {
    ...payload,
    disable_dedupe: true
  };
}

function parseDelimitedText(value: string): string[] {
  return value
    .split(/[\n,]/g)
    .map((item) => item.trim())
    .filter(Boolean);
}

function serializeDelimitedText(values: string[]): string {
  return values.join(", ");
}

function parseLineSeparatedText(value: string): string[] {
  return value
    .split(/[\n;]/g)
    .map((item) => item.trim())
    .filter(Boolean);
}

function serializeLineSeparatedText(values: string[]): string {
  return values.join("\n");
}

function normalizeJobWorkMode(value: string): JobWorkModeOption | null {
  const normalized = value.trim().toLowerCase().replace("_", "-");
  if (normalized === "remote" || normalized === "hybrid") return normalized;
  if (normalized === "onsite" || normalized === "on-site") return "onsite";
  return null;
}

function normalizeFreshnessPreference(value: unknown): string {
  if (typeof value !== "string") return "off";
  const normalized = value.trim().toLowerCase().replace("-", "_").replace(" ", "_");
  return JOB_FRESHNESS_PREFERENCE_OPTIONS.includes(normalized as (typeof JOB_FRESHNESS_PREFERENCE_OPTIONS)[number])
    ? normalized
    : "off";
}

function parseJobsWatcherFormFromPayload(rawPayloadJson: string | null | undefined): JobsWatcherFormState {
  const payload = parsePayloadObject(rawPayloadJson);
  const request = payload.request && typeof payload.request === "object" && !Array.isArray(payload.request)
    ? payload.request as Record<string, unknown>
    : payload;

  const titles = parseTextList(request.titles);
  const desiredTitle = typeof request.desired_title === "string" ? request.desired_title.trim() : "";
  if (!titles.length && desiredTitle) titles.push(desiredTitle);
  const titleKeywords = parseTextList(request.desired_title_keywords);
  if (!titles.length && titleKeywords.length) titles.push(...titleKeywords);
  const query = typeof request.query === "string" ? request.query.trim() : "";
  if (!titles.length && query) titles.push(query);

  const keywords = parseTextList(request.keywords);
  if (!keywords.length) keywords.push(...titleKeywords);
  const excludedKeywords = parseTextList(request.excluded_keywords);

  const locations = parseTextList(request.locations);
  const location = typeof request.location === "string" ? request.location.trim() : "";
  if (!locations.length && location) locations.push(location);

  const rawModes = [
    ...parseTextList(request.work_mode_preference),
    ...parseTextList(request.work_modes),
    ...parseTextList(request.remote_preference)
  ];
  const remotePreference: Record<JobWorkModeOption, boolean> = { remote: false, hybrid: false, onsite: false };
  rawModes.forEach((value) => {
    const normalized = normalizeJobWorkMode(value);
    if (normalized) remotePreference[normalized] = true;
  });
  if (!remotePreference.remote && !remotePreference.hybrid && !remotePreference.onsite) {
    remotePreference.remote = true;
    remotePreference.hybrid = true;
  }

  const rawSources = [
    ...parseTextList(request.enabled_sources),
    ...parseTextList(request.sources),
    ...parseTextList(request.boards)
  ].map((item) => item.toLowerCase());
  const enabledSources: Record<JobSourceOption, boolean> = {
    linkedin: false,
    indeed: false,
    glassdoor: false,
    handshake: false
  };
  rawSources.forEach((source) => {
    if (source in enabledSources) {
      enabledSources[source as JobSourceOption] = true;
    }
  });
  if (!Object.values(enabledSources).some(Boolean)) {
    JOB_SOURCE_OPTIONS.forEach((source) => {
      enabledSources[source] = true;
    });
  }

  const minimumSalaryRaw = request.minimum_salary ?? request.desired_salary_min;
  const minimumSalaryText = minimumSalaryRaw == null ? "" : String(minimumSalaryRaw);

  const experienceLevelRaw =
    typeof request.experience_level === "string" && request.experience_level.trim()
      ? request.experience_level.trim().toLowerCase()
      : parseTextList(request.experience_levels)[0]?.toLowerCase() || "";
  const experienceLevel = JOB_EXPERIENCE_LEVEL_OPTIONS.includes(experienceLevelRaw as (typeof JOB_EXPERIENCE_LEVEL_OPTIONS)[number])
    ? experienceLevelRaw
    : "";

  const resultLimitRaw = request.result_limit_per_source ?? request.max_jobs_per_source;
  const resultLimitPerSourceText = resultLimitRaw == null ? "25" : String(resultLimitRaw);

  const shortlistCountRaw = request.shortlist_max_items ?? request.shortlist_count;
  const shortlistCountText = shortlistCountRaw == null ? "5" : String(shortlistCountRaw);

  return {
    desiredTitlesText: serializeDelimitedText(titles),
    keywordsText: serializeDelimitedText(keywords),
    excludedKeywordsText: serializeDelimitedText(excludedKeywords),
    preferredLocationsText: serializeLineSeparatedText(locations),
    remotePreference,
    minimumSalaryText,
    experienceLevel,
    enabledSources,
    resultLimitPerSourceText,
    shortlistCountText,
    freshnessPreference: normalizeFreshnessPreference(
      request.shortlist_freshness_preference ?? request.freshness_preference
    )
  };
}

function summarizeNotificationBehavior(
  taskType: string,
  watcher: Watcher | null,
  fallbackPayload?: Record<string, unknown>
): string | null {
  const behavior = watcher?.notification_behavior && typeof watcher.notification_behavior === "object"
    ? watcher.notification_behavior as Record<string, unknown>
    : {};

  const payload = watcher ? parsePayloadObject(watcher.payload_json) : (fallbackPayload || {});
  const channels = Array.isArray(payload.channels)
    ? payload.channels
        .map((value) => (typeof value === "string" ? value.trim() : ""))
        .filter(Boolean)
    : [];
  const legacyChannel = typeof payload.channel === "string" ? payload.channel.trim() : "";
  if (channels.length === 0 && legacyChannel) channels.push(legacyChannel);

  const mode = typeof behavior.mode === "string" ? behavior.mode.trim() : "";
  const behaviorChannel = typeof behavior.channel === "string" ? behavior.channel.trim() : "";
  const severity = typeof payload.severity === "string" ? payload.severity.trim() : "";
  const dedupeTtl = Number(payload.dedupe_ttl_seconds);

  const parts: string[] = [];
  if (channels.length > 0) parts.push(`channels: ${channels.join(", ")}`);
  if (severity) parts.push(`severity: ${severity}`);
  if (mode) parts.push(`mode: ${mode}`);
  if (behaviorChannel) parts.push(`target: ${behaviorChannel}`);
  if (Number.isFinite(dedupeTtl) && dedupeTtl > 0) parts.push(`dedupe ttl: ${Math.trunc(dedupeTtl)}s`);

  const hasNotificationSignal = taskType === NOTIFY_TASK_TYPE || parts.length > 0;
  return hasNotificationSignal ? (parts.join(" · ") || "Notification workflow configured") : null;
}

function deriveLastResultSummary(taskType: string, watcher: Watcher | null): string {
  const outcome = watcher?.last_outcome_summary;
  const run = watcher?.last_run_summary;

  if (outcome?.message) {
    return summarizeText(outcome.message, 180);
  }

  const outcomeBits: string[] = [];
  if (outcome?.status) outcomeBits.push(String(outcome.status).replace(/_/g, " "));
  if (outcome?.artifact_type) outcomeBits.push(`artifact ${outcome.artifact_type}`);
  if (outcome?.created_at) outcomeBits.push(`captured ${formatIso(outcome.created_at)}`);
  if (outcomeBits.length > 0) return outcomeBits.join(" · ");

  if (run?.error) return summarizeText(run.error, 170);
  if (!run) return "No result summary yet.";

  const normalizedStatus = String(run.task_status || "").toLowerCase();
  if (SUCCESS_TASK_STATUSES.has(normalizedStatus)) {
    if (taskType === "deals_scan_v1") return "Latest scan completed; unicorn summary available in run artifacts.";
    if (taskType.startsWith("jobs_")) return "Latest jobs pipeline stage completed; check run artifacts for stage outputs.";
    if (taskType === NOTIFY_TASK_TYPE) return "Latest notification flow completed.";
    return "Latest run completed successfully.";
  }
  if (FAILED_TASK_STATUSES.has(normalizedStatus)) return "Latest run failed. Inspect run error and retry conditions.";
  return "Run outcome recorded. Open Runs for full execution details.";
}

function deriveNextLikelyAction(input: {
  watcher: Watcher | null;
  taskType: string;
  plannerEnabled: boolean;
  executionEnabled: boolean;
  requiresApproval: boolean;
  approved: boolean;
  lastRunStatus: string | null;
}): string {
  const {
    watcher,
    taskType,
    plannerEnabled,
    executionEnabled,
    requiresApproval,
    approved,
    lastRunStatus
  } = input;
  const normalizedLastRun = String(lastRunStatus || "").toLowerCase();

  if (!watcher) return "Create managed watcher to schedule autonomous runs; manual run remains available.";
  if (!watcher.enabled) return "Enable watcher to resume scheduled execution.";
  if (!plannerEnabled) return "Planner is paused. Run manually now or enable planner scheduling.";
  if (executionEnabled && requiresApproval && !approved) return "Approve execution to allow autonomous task launches.";
  if (FAILED_TASK_STATUSES.has(normalizedLastRun)) return "Open Runs, inspect latest failure details, and adjust watcher configuration.";
  if (taskType === NOTIFY_TASK_TYPE) return "Verify notification delivery destination and dedupe behavior.";
  if (executionEnabled) return "Await next scheduler cycle and monitor the next run outcome.";
  return "Planner is in recommendation mode; execute manually or switch to execution mode.";
}

function buildWorkflowInsight(input: {
  id: string;
  workflowName: string;
  taskType: string;
  description: string;
  watcher: Watcher | null;
  fallbackIntervalSeconds: number;
  fallbackPayload?: Record<string, unknown>;
  plannerEnabled: boolean;
  executionEnabled: boolean;
  requiresApproval: boolean;
  approved: boolean;
}): WorkflowInsight {
  const {
    id,
    workflowName,
    taskType,
    description,
    watcher,
    fallbackIntervalSeconds,
    fallbackPayload,
    plannerEnabled,
    executionEnabled,
    requiresApproval,
    approved
  } = input;

  const stateLabel = watcher ? (watcher.enabled ? "enabled" : "disabled") : "manual";
  const effectiveIntervalLabel = watcher
    ? `${Math.max(60, Number(watcher.interval_seconds) || fallbackIntervalSeconds)}s`
    : `${Math.max(60, Number(fallbackIntervalSeconds) || 60)}s (manual blueprint)`;
  const lastRunStatus = watcher?.last_run_summary?.task_status || null;
  const lastRunTimeLabel = watcher?.last_run_summary?.task_updated_at
    ? formatIso(watcher.last_run_summary.task_updated_at)
    : "Not run yet";
  const lastRunOutcomeLabel = lastRunStatus ? String(lastRunStatus).replace(/_/g, " ") : "not run";

  return {
    id,
    workflowName,
    taskType,
    description,
    stateLabel,
    effectiveIntervalLabel,
    lastRunTimeLabel,
    lastRunStatusRaw: lastRunStatus,
    lastRunOutcomeLabel,
    lastResultSummary: deriveLastResultSummary(taskType, watcher),
    nextLikelyAction: deriveNextLikelyAction({
      watcher,
      taskType,
      plannerEnabled,
      executionEnabled,
      requiresApproval,
      approved,
      lastRunStatus
    }),
    notificationBehaviorLabel: summarizeNotificationBehavior(taskType, watcher, fallbackPayload),
    watcherId: watcher?.id || null
  };
}

function numberFromSummary(summary: Record<string, unknown> | undefined, key: string): number {
  const raw = summary?.[key];
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

function resolvePrimaryWatcher(primary: (typeof PRIMARY_WATCHERS)[number], watchers: Watcher[]): Watcher | null {
  const byId = watchers.find((row) => row.id === primary.watcherId);
  if (byId) return byId;

  const sameType = watchers.filter((row) => row.task_type === primary.taskType);
  if (sameType.length === 0) return null;

  const sorted = sameType.slice().sort((a, b) => {
    if (a.enabled !== b.enabled) return a.enabled ? -1 : 1;
    if (a.priority !== b.priority) return a.priority - b.priority;
    return new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime();
  });
  return sorted[0] || null;
}

export function WorkflowsPage(): JSX.Element {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const highlightedWatcherId = searchParams.get("watcher") || searchParams.get("template");

  const configQuery = usePlannerConfig();
  const plannerStatusQuery = usePlannerStatus();
  const watchersQuery = useWatchers(120);

  const patchConfigMutation = usePatchPlannerConfigMutation();
  const resetConfigMutation = useResetPlannerConfigMutation();
  const runPlannerOnceMutation = useRunPlannerOnceMutation();
  const createWatcherMutation = useCreateWatcherMutation();
  const deleteWatcherMutation = useDeleteWatcherMutation();
  const patchWatcherMutation = usePatchWatcherMutation();
  const saveRtxMutation = useSaveRtxPresetMutation();
  const saveJobsMutation = useSaveJobsPresetMutation();
  const createTaskMutation = useCreateTaskMutation();

  const [intervalSec, setIntervalSec] = useState(300);
  const [maxCreate, setMaxCreate] = useState(1);
  const [maxExecute, setMaxExecute] = useState(2);
  const [lastManualRunAt, setLastManualRunAt] = useState<string | null>(null);
  const [primaryIntervals, setPrimaryIntervals] = useState<PrimaryIntervals>(DEFAULT_PRIMARY_INTERVALS);

  const [createName, setCreateName] = useState("New Watcher");
  const [createTaskType, setCreateTaskType] = useState("deals_scan_v1");
  const [createPayload, setCreatePayload] = useState('{"source":"watcher-template","collectors_enabled":true}');
  const [createIntervalSec, setCreateIntervalSec] = useState(300);
  const [createPayloadError, setCreatePayloadError] = useState<string | null>(null);

  const [editWatcherId, setEditWatcherId] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [editPayload, setEditPayload] = useState("");
  const [editIntervalSec, setEditIntervalSec] = useState(300);
  const [editPayloadError, setEditPayloadError] = useState<string | null>(null);
  const [selectedWorkflowDetailId, setSelectedWorkflowDetailId] = useState<PrimaryWatcherId>("deals");
  const [jobsForm, setJobsForm] = useState<JobsWatcherFormState>(DEFAULT_JOBS_WATCHER_FORM_STATE);
  const [jobsFormError, setJobsFormError] = useState<string | null>(null);

  useEffect(() => {
    if (!configQuery.data) return;
    setIntervalSec(configQuery.data.interval_sec);
    setMaxCreate(configQuery.data.max_create_per_cycle);
    setMaxExecute(configQuery.data.max_execute_per_cycle);
  }, [configQuery.data]);

  const watchers = useMemo(() => watchersQuery.data || [], [watchersQuery.data]);

  const primaryWatcherMap = useMemo(() => {
    return PRIMARY_WATCHERS.reduce<Record<PrimaryWatcherId, Watcher | null>>((acc, primary) => {
      acc[primary.id] = resolvePrimaryWatcher(primary, watchers);
      return acc;
    }, { deals: null, jobs: null, notify: null });
  }, [watchers]);

  useEffect(() => {
    if (!highlightedWatcherId) return;
    const primary = PRIMARY_WATCHERS.find((row) => row.watcherId === highlightedWatcherId);
    if (primary) setSelectedWorkflowDetailId(primary.id);

    const watcher = watchers.find((row) => row.id === highlightedWatcherId);
    if (!watcher) return;
    setEditWatcherId(watcher.id);
    setEditName(watcher.name);
    setEditPayload(watcher.payload_json);
    setEditIntervalSec(Math.max(60, Number(watcher.min_interval_seconds) || 300));
    setEditPayloadError(null);
  }, [highlightedWatcherId, watchers]);

  useEffect(() => {
    setPrimaryIntervals((prev) => {
      const next = { ...prev };
      let changed = false;
      PRIMARY_WATCHERS.forEach((primary) => {
        const watcher = primaryWatcherMap[primary.id];
        if (!watcher) return;
        const interval = Math.max(60, Number(watcher.interval_seconds) || primary.defaultInterval);
        if (next[primary.id] !== interval) {
          next[primary.id] = interval;
          changed = true;
        }
      });
      return changed ? next : prev;
    });
  }, [primaryWatcherMap]);

  const enabledWatcherCount = watchers.filter((row) => row.enabled).length;
  const plannerEnabled = Boolean(configQuery.data?.enabled);
  const executionEnabled = Boolean(configQuery.data?.execution_enabled);
  const requiresApproval = Boolean(configQuery.data?.require_approval);
  const approved = Boolean(configQuery.data?.approved);

  const recentSummary = plannerStatusQuery.data?.recent_summary_24h;
  const cycles24h = numberFromSummary(recentSummary, "cycles") || numberFromSummary(recentSummary, "ticks");
  const executed24h = numberFromSummary(recentSummary, "executed_actions");

  const queryError = [configQuery.error, plannerStatusQuery.error, watchersQuery.error].find(Boolean);
  const mutationError = [
    patchConfigMutation.error,
    resetConfigMutation.error,
    runPlannerOnceMutation.error,
    createWatcherMutation.error,
    deleteWatcherMutation.error,
    patchWatcherMutation.error,
    saveRtxMutation.error,
    saveJobsMutation.error,
    createTaskMutation.error
  ].find(Boolean);

  const refreshWorkflowData = (): void => {
    void Promise.all([configQuery.refetch(), plannerStatusQuery.refetch(), watchersQuery.refetch()]);
  };

  const selectWatcherForEdit = (watcher: Watcher): void => {
    setEditWatcherId(watcher.id);
    setEditName(watcher.name);
    setEditPayload(watcher.payload_json);
    setEditIntervalSec(Math.max(60, Number(watcher.min_interval_seconds) || 300));
    setEditPayloadError(null);

    const primary = PRIMARY_WATCHERS.find((row) => row.watcherId === watcher.id || row.taskType === watcher.task_type);
    if (primary) setSelectedWorkflowDetailId(primary.id);
  };

  const runBlueprint = (taskType: string, payload: Record<string, unknown>): void => {
    const payloadForRun = withNotifyTestDedupeBypass(taskType, payload);
    createTaskMutation.mutate(
      { task_type: taskType, payload_json: JSON.stringify(payloadForRun), max_attempts: 3 },
      { onSuccess: () => navigate("/runs") }
    );
  };

  const setPrimaryIntervalInput = (primaryId: PrimaryWatcherId, raw: string): void => {
    const next = Math.max(60, Number(raw) || DEFAULT_PRIMARY_INTERVALS[primaryId]);
    setPrimaryIntervals((prev) => ({ ...prev, [primaryId]: next }));
  };

  const setJobsSourceEnabled = (source: JobSourceOption, enabled: boolean): void => {
    setJobsForm((prev) => ({
      ...prev,
      enabledSources: {
        ...prev.enabledSources,
        [source]: enabled
      }
    }));
    setJobsFormError(null);
  };

  const setJobsWorkModeEnabled = (mode: JobWorkModeOption, enabled: boolean): void => {
    setJobsForm((prev) => ({
      ...prev,
      remotePreference: {
        ...prev.remotePreference,
        [mode]: enabled
      }
    }));
    setJobsFormError(null);
  };

  const buildJobsPresetInput = (intervalSeconds: number, enabled: boolean): {
    error?: string;
    payload?: {
      interval_seconds: number;
      desired_title?: string | null;
      desired_titles?: string[] | null;
      keywords?: string[] | null;
      excluded_keywords?: string[] | null;
      preferred_locations?: string[] | null;
      remote_preference?: string[] | null;
      minimum_salary?: number | null;
      desired_salary_min?: number | null;
      experience_level?: string | null;
      enabled_sources?: string[] | null;
      boards?: string[] | null;
      result_limit_per_source?: number | null;
      shortlist_count?: number | null;
      freshness_preference?: string | null;
      location?: string | null;
      enabled?: boolean;
    };
  } => {
    const desiredTitles = parseDelimitedText(jobsForm.desiredTitlesText);
    const keywords = parseDelimitedText(jobsForm.keywordsText);
    const excludedKeywords = parseDelimitedText(jobsForm.excludedKeywordsText);
    const preferredLocations = parseLineSeparatedText(jobsForm.preferredLocationsText);
    const remotePreference = JOB_WORK_MODE_OPTIONS.filter((mode) => jobsForm.remotePreference[mode]);
    const enabledSources = JOB_SOURCE_OPTIONS.filter((source) => jobsForm.enabledSources[source]);

    if (enabledSources.length === 0) {
      return { error: "Enable at least one source." };
    }

    const minimumSalaryRaw = jobsForm.minimumSalaryText.trim().replace(/[$,]/g, "");
    let minimumSalary: number | null = null;
    if (minimumSalaryRaw) {
      const parsed = Number(minimumSalaryRaw);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        return { error: "Minimum salary must be a positive number." };
      }
      minimumSalary = parsed;
    }

    const resultLimitRaw = jobsForm.resultLimitPerSourceText.trim();
    const resultLimitPerSource = Number(resultLimitRaw || "25");
    if (!Number.isFinite(resultLimitPerSource) || !Number.isInteger(resultLimitPerSource) || resultLimitPerSource < 1 || resultLimitPerSource > 100) {
      return { error: "Result limit per source must be an integer between 1 and 100." };
    }

    const shortlistCountRaw = jobsForm.shortlistCountText.trim();
    const shortlistCount = Number(shortlistCountRaw || "5");
    if (!Number.isFinite(shortlistCount) || !Number.isInteger(shortlistCount) || shortlistCount < 1 || shortlistCount > 10) {
      return { error: "Top-N shortlist count must be an integer between 1 and 10." };
    }

    const experienceLevel = JOB_EXPERIENCE_LEVEL_OPTIONS.includes(
      jobsForm.experienceLevel as (typeof JOB_EXPERIENCE_LEVEL_OPTIONS)[number]
    )
      ? jobsForm.experienceLevel
      : "";
    const freshnessPreference = normalizeFreshnessPreference(jobsForm.freshnessPreference);

    const payload = {
      interval_seconds: Math.max(60, Number(intervalSeconds) || 300),
      desired_title: desiredTitles[0] || null,
      desired_titles: desiredTitles.length > 0 ? desiredTitles : null,
      keywords: keywords.length > 0 ? keywords : null,
      excluded_keywords: excludedKeywords.length > 0 ? excludedKeywords : null,
      preferred_locations: preferredLocations.length > 0 ? preferredLocations : null,
      remote_preference: remotePreference.length > 0 ? remotePreference : null,
      minimum_salary: minimumSalary,
      desired_salary_min: minimumSalary,
      experience_level: experienceLevel || null,
      enabled_sources: enabledSources,
      boards: enabledSources,
      result_limit_per_source: Math.trunc(resultLimitPerSource),
      shortlist_count: Math.trunc(shortlistCount),
      freshness_preference: freshnessPreference,
      location: preferredLocations[0] || null,
      enabled
    };
    return { payload };
  };

  const saveJobsWorkflowConfiguration = (): void => {
    const safeInterval = Math.max(60, Number(primaryIntervals.jobs) || DEFAULT_PRIMARY_INTERVALS.jobs);
    const preset = buildJobsPresetInput(safeInterval, primaryWatcherMap.jobs?.enabled ?? true);
    if (preset.error || !preset.payload) {
      setJobsFormError(preset.error || "Invalid jobs watcher configuration.");
      return;
    }
    setJobsFormError(null);
    saveJobsMutation.mutate(preset.payload);
  };

  const upsertPrimaryWatcher = (primaryId: PrimaryWatcherId, intervalSeconds: number): void => {
    const primary = PRIMARY_WATCHERS.find((row) => row.id === primaryId);
    if (!primary) return;

    const safeInterval = Math.max(60, Number(intervalSeconds) || primary.defaultInterval);
    if (primaryId === "deals") {
      saveRtxMutation.mutate({ interval_seconds: safeInterval, gpu_max_price: 2000, pc_max_price: 4000, enabled: true });
      return;
    }

    if (primaryId === "jobs") {
      saveJobsMutation.mutate({
        interval_seconds: safeInterval,
        desired_title: "Software Engineer",
        desired_titles: ["Software Engineer"],
        preferred_locations: ["United States"],
        remote_preference: ["remote", "hybrid"],
        enabled_sources: ["linkedin", "indeed", "glassdoor", "handshake"],
        boards: ["linkedin", "indeed", "glassdoor", "handshake"],
        result_limit_per_source: 25,
        shortlist_count: 5,
        freshness_preference: "off",
        enabled: true
      });
      return;
    }

    const existing = primaryWatcherMap.notify;
    if (existing) {
      const patch: WatcherPatchInput = {
        enabled: true,
        min_interval_seconds: safeInterval,
        notification_behavior: existing.notification_behavior || { mode: "digest", channel: "operator_default" }
      };
      const normalizedPayload = normalizeNotifyWorkflowPayload(existing.payload_json);
      if (normalizedPayload !== existing.payload_json) {
        patch.payload_json = normalizedPayload;
      }
      patchWatcherMutation.mutate({
        watcherId: existing.id,
        patch
      });
      selectWatcherForEdit(existing);
      return;
    }

    createWatcherMutation.mutate({
      id: primary.watcherId,
      name: primary.name,
      task_type: primary.taskType,
      payload_json: normalizeNotifyWorkflowPayload(JSON.stringify(primary.payload)),
      min_interval_seconds: safeInterval,
      max_attempts: 3,
      enabled: true,
      priority: 30,
      notification_behavior: {
        mode: "digest",
        channel: "operator_default"
      },
      metadata: {
        watcher_category: "ops",
        description: "Daily ops operator notification watcher"
      }
    });
  };

  const savePrimaryWatcherInterval = (primaryId: PrimaryWatcherId): void => {
    const primary = PRIMARY_WATCHERS.find((row) => row.id === primaryId);
    if (!primary) return;

    const watcher = primaryWatcherMap[primaryId];
    const nextInterval = Math.max(60, Number(primaryIntervals[primaryId]) || primary.defaultInterval);
    if (watcher) {
      const patch: WatcherPatchInput = { min_interval_seconds: nextInterval };
      if (primaryId === "notify") {
        const normalizedPayload = normalizeNotifyWorkflowPayload(watcher.payload_json);
        if (normalizedPayload !== watcher.payload_json) {
          patch.payload_json = normalizedPayload;
        }
      }
      patchWatcherMutation.mutate({
        watcherId: watcher.id,
        patch
      });
      return;
    }

    upsertPrimaryWatcher(primaryId, nextInterval);
  };

  const createWatcher = (): void => {
    if (!createName.trim() || !createTaskType.trim()) {
      setCreatePayloadError("Watcher name and task type are required.");
      return;
    }

    try {
      JSON.parse(createPayload);
    } catch {
      setCreatePayloadError("Watcher payload must be valid JSON.");
      return;
    }

    setCreatePayloadError(null);
    createWatcherMutation.mutate({
      name: createName,
      task_type: createTaskType,
      payload_json: createPayload,
      min_interval_seconds: Math.max(60, createIntervalSec),
      max_attempts: 3,
      enabled: true,
      priority: 100,
      metadata: {
        watcher_category: "custom"
      }
    });
  };

  const saveWatcherEdit = (): void => {
    if (!editWatcherId) return;
    if (!editName.trim()) {
      setEditPayloadError("Watcher name is required.");
      return;
    }

    try {
      JSON.parse(editPayload);
    } catch {
      setEditPayloadError("Edited payload must be valid JSON.");
      return;
    }

    setEditPayloadError(null);
    patchWatcherMutation.mutate({
      watcherId: editWatcherId,
      patch: {
        name: editName,
        payload_json: editPayload,
        min_interval_seconds: Math.max(60, editIntervalSec)
      }
    });
  };

  const primaryWorkflowInsights = useMemo(() => {
    return PRIMARY_WATCHERS.map((primary) =>
      buildWorkflowInsight({
        id: primary.id,
        workflowName: primaryWatcherMap[primary.id]?.name || primary.name,
        taskType: primary.taskType,
        description: primary.description,
        watcher: primaryWatcherMap[primary.id],
        fallbackIntervalSeconds: primaryIntervals[primary.id] || primary.defaultInterval,
        fallbackPayload: primary.payload,
        plannerEnabled,
        executionEnabled,
        requiresApproval,
        approved
      })
    );
  }, [primaryWatcherMap, primaryIntervals, plannerEnabled, executionEnabled, requiresApproval, approved]);

  const watcherInsightsById = useMemo(() => {
    const byId = new Map<string, WorkflowInsight>();
    watchers.forEach((watcher) => {
      const primary = PRIMARY_WATCHERS.find((row) => row.watcherId === watcher.id || row.taskType === watcher.task_type);
      const metadata = watcher.metadata && typeof watcher.metadata === "object" ? watcher.metadata as Record<string, unknown> : {};
      const metadataDescription = typeof metadata.description === "string" ? metadata.description.trim() : "";

      const insight = buildWorkflowInsight({
        id: watcher.id,
        workflowName: watcher.name,
        taskType: watcher.task_type,
        description: metadataDescription || primary?.description || "Custom watcher automation.",
        watcher,
        fallbackIntervalSeconds: Math.max(60, Number(watcher.interval_seconds) || 300),
        fallbackPayload: primary?.payload,
        plannerEnabled,
        executionEnabled,
        requiresApproval,
        approved
      });
      byId.set(watcher.id, insight);
    });
    return byId;
  }, [watchers, plannerEnabled, executionEnabled, requiresApproval, approved]);

  const selectedWorkflowInsight =
    primaryWorkflowInsights.find((row) => row.id === selectedWorkflowDetailId) || primaryWorkflowInsights[0] || null;
  const selectedPrimaryWorkflow = selectedWorkflowInsight
    ? PRIMARY_WATCHERS.find((row) => row.id === selectedWorkflowInsight.id)
    : null;
  const selectedPrimaryWatcher = selectedPrimaryWorkflow ? primaryWatcherMap[selectedPrimaryWorkflow.id] : null;
  const selectedPrimaryPayloadJson = useMemo(() => {
    if (selectedPrimaryWatcher?.payload_json) return selectedPrimaryWatcher.payload_json;
    if (!selectedPrimaryWorkflow) return null;
    return JSON.stringify(selectedPrimaryWorkflow.payload);
  }, [selectedPrimaryWatcher?.payload_json, selectedPrimaryWorkflow]);
  const selectedEditWatcher = editWatcherId ? (watchers.find((row) => row.id === editWatcherId) || null) : null;
  const editIsJobsWatcher = selectedEditWatcher?.task_type === "jobs_collect_v1";

  useEffect(() => {
    if (selectedWorkflowDetailId !== "jobs") return;
    setJobsForm(parseJobsWatcherFormFromPayload(selectedPrimaryPayloadJson));
    setJobsFormError(null);
  }, [selectedWorkflowDetailId, selectedPrimaryPayloadJson]);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Workflows"
        subtitle="Manage automation watchers as first-class monitored objects: planner policy, watcher inventory, and outcome state."
        actions={<Button variant="outline" onClick={refreshWorkflowData}>Refresh</Button>}
      />

      {queryError ? (
        <ErrorPanel title="Workflow data failed to load" message={errorMessage(queryError)} onAction={refreshWorkflowData} />
      ) : null}
      {mutationError ? <ErrorPanel title="Workflow update failed" message={errorMessage(mutationError)} /> : null}

      <section>
        <Card>
          <CardHeader>
            <CardTitle>Planner Control Strip</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex flex-wrap items-center gap-2 text-xs">
              <span className="text-muted-foreground">planner</span>
              <StatusBadge status={plannerEnabled ? "enabled" : "paused"} />
              <span className="text-muted-foreground">mode</span>
              <StatusBadge status={executionEnabled ? "execution" : "recommendation"} />
              <span className="text-muted-foreground">approval</span>
              <StatusBadge status={!requiresApproval ? "not_required" : approved ? "approved" : "awaiting"} />
              <span className="text-muted-foreground">interval</span>
              <span className="rounded border border-border bg-muted/35 px-2 py-0.5 text-[11px] font-medium">{intervalSec}s</span>
              <span className="text-muted-foreground">active watchers</span>
              <span className="rounded border border-border bg-muted/35 px-2 py-0.5 text-[11px] font-medium">{enabledWatcherCount}</span>
            </div>

            <div className="flex flex-wrap gap-2">
              <Button onClick={() => patchConfigMutation.mutate({ enabled: !plannerEnabled })}>
                {plannerEnabled ? "Pause Planner" : "Enable Planner"}
              </Button>
              <Button variant="secondary" onClick={() => patchConfigMutation.mutate({ execution_enabled: !executionEnabled })}>
                {executionEnabled ? "Switch to Recommendation" : "Switch to Execution"}
              </Button>
              {requiresApproval ? (
                <Button variant="secondary" onClick={() => patchConfigMutation.mutate({ approved: !approved })}>
                  {approved ? "Revoke Approval" : "Approve Execution"}
                </Button>
              ) : null}
              <Button
                onClick={() =>
                  runPlannerOnceMutation.mutate(undefined, {
                    onSuccess: () => {
                      setLastManualRunAt(new Date().toISOString());
                    }
                  })
                }
                disabled={runPlannerOnceMutation.isPending}
              >
                <Play className="h-3.5 w-3.5" />
                {runPlannerOnceMutation.isPending ? "Running…" : "Run Planner Now"}
              </Button>
            </div>

            <div className="grid gap-2 sm:grid-cols-[180px_auto] sm:items-end">
              <div>
                <Label>Planner interval (seconds)</Label>
                <Input type="number" value={intervalSec} onChange={(e) => setIntervalSec(Math.max(30, Number(e.target.value) || 300))} />
              </div>
              <Button variant="outline" onClick={() => patchConfigMutation.mutate({ interval_sec: intervalSec })}>
                Save Interval
              </Button>
            </div>

            <div className="text-xs text-muted-foreground">
              {plannerEnabled && executionEnabled && requiresApproval && !approved
                ? "Execution is enabled but awaiting approval."
                : plannerEnabled
                  ? "Planner is active."
                  : "Planner is paused."}
              {cycles24h > 0 || executed24h > 0
                ? ` Recent 24h: ${cycles24h} cycles, ${executed24h} executed actions.`
                : " No planner activity recorded in the current window."}
              {lastManualRunAt ? ` Last manual run: ${formatIso(lastManualRunAt)}.` : ""}
              {" Per-watcher intervals override planner cadence when watcher templates are configured."}
            </div>
          </CardContent>
        </Card>
      </section>

      <section>
        <SectionHeader title="Primary Watchers" subtitle="Operate core saved watchers directly from managed cards." />
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {PRIMARY_WATCHERS.map((primary) => {
            const watcher = primaryWatcherMap[primary.id];
            const insight = primaryWorkflowInsights.find((row) => row.id === primary.id) || null;
            const watcherIntervalSec = primaryIntervals[primary.id] || primary.defaultInterval;
            const runSummary = watcher?.last_run_summary;
            const outcomeSummary = watcher?.last_outcome_summary;

            const plannerStateMessage =
              !plannerEnabled
                ? "Planner is paused. Manual runs remain available."
                : executionEnabled
                  ? requiresApproval && !approved
                    ? "Awaiting planner approval before autonomous execution."
                    : "Autonomous execution is active."
                  : "Recommendation mode: planner suggests without auto-execution.";

            const intervalMessage = `Watcher interval: ${watcherIntervalSec}s${watcher ? " (managed)" : " (manual only)"}.`;
            const recentRunMessage = runSummary
              ? `Last run ${formatIso(runSummary.task_updated_at)} (${String(runSummary.task_status).replace(/_/g, " ")})`
              : "No recent run recorded for this watcher.";
            const runError =
              runSummary && FAILED_TASK_STATUSES.has(String(runSummary.task_status).toLowerCase())
                ? runSummary.error || outcomeSummary?.message || "Most recent run reported an issue."
                : null;

            return (
              <WorkflowCard
                key={primary.id}
                name={watcher?.name || primary.name}
                taskType={primary.taskType}
                status={watcher ? (watcher.enabled ? "enabled" : "disabled") : "manual"}
                cadence={insight?.effectiveIntervalLabel || (watcher ? `${watcher.interval_seconds}s` : "manual")}
                description={primary.description}
                stateMessage={`${plannerStateMessage} ${intervalMessage}`}
                recentRunMessage={recentRunMessage}
                lastRunStatus={runSummary?.task_status || null}
                lastRunAt={runSummary?.task_updated_at ? formatIso(runSummary.task_updated_at) : null}
                lastResultSummary={insight?.lastResultSummary}
                nextLikelyAction={insight?.nextLikelyAction}
                notificationBehavior={insight?.notificationBehaviorLabel}
                errorMessage={runError}
                onRun={() => runBlueprint(primary.taskType, primary.payload)}
                onConfigure={() => {
                  if (watcher) {
                    selectWatcherForEdit(watcher);
                    return;
                  }
                  upsertPrimaryWatcher(primary.id, watcherIntervalSec);
                }}
                actions={
                  <div className="flex flex-wrap items-center gap-2">
                    <Input
                      type="number"
                      value={watcherIntervalSec}
                      onChange={(e) => setPrimaryIntervalInput(primary.id, e.target.value)}
                      className="h-8 w-[104px]"
                    />
                    <Button size="sm" variant="outline" onClick={() => savePrimaryWatcherInterval(primary.id)}>
                      Save Interval
                    </Button>
                    <Button size="sm" variant="secondary" onClick={() => setSelectedWorkflowDetailId(primary.id)}>
                      Details
                    </Button>
                    {watcher ? (
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => patchWatcherMutation.mutate({ watcherId: watcher.id, patch: { enabled: !watcher.enabled } })}
                      >
                        {watcher.enabled ? "Disable" : "Enable"}
                      </Button>
                    ) : (
                      <Button size="sm" variant="outline" onClick={() => upsertPrimaryWatcher(primary.id, watcherIntervalSec)}>
                        Create Managed Watcher
                      </Button>
                    )}
                  </div>
                }
              />
            );
          })}
        </div>
      </section>

      <section>
        <Card>
          <CardHeader>
            <CardTitle>Workflow Detail</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {selectedWorkflowInsight ? (
              <>
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="space-y-1">
                    <div className="text-sm font-semibold tracking-tight">{selectedWorkflowInsight.workflowName}</div>
                    <div className="text-xs text-muted-foreground">{selectedWorkflowInsight.description}</div>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <StatusBadge status={selectedWorkflowInsight.stateLabel} />
                    <StatusBadge status={selectedWorkflowInsight.lastRunStatusRaw || selectedWorkflowInsight.lastRunOutcomeLabel} />
                  </div>
                </div>

                <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                  <div className="rounded-md border border-border/70 bg-muted/20 px-2 py-1.5 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Workflow Name</div>
                    <div className="mt-1 text-foreground">{selectedWorkflowInsight.workflowName}</div>
                  </div>
                  <div className="rounded-md border border-border/70 bg-muted/20 px-2 py-1.5 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Task Type</div>
                    <div className="mt-1 font-mono text-[11px] text-foreground">{selectedWorkflowInsight.taskType}</div>
                  </div>
                  <div className="rounded-md border border-border/70 bg-muted/20 px-2 py-1.5 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Effective Interval</div>
                    <div className="mt-1 text-foreground">{selectedWorkflowInsight.effectiveIntervalLabel}</div>
                  </div>
                  <div className="rounded-md border border-border/70 bg-muted/20 px-2 py-1.5 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Last Run Time</div>
                    <div className="mt-1 text-foreground">{selectedWorkflowInsight.lastRunTimeLabel}</div>
                  </div>
                  <div className="rounded-md border border-border/70 bg-muted/20 px-2 py-1.5 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Last Run Outcome</div>
                    <div className="mt-1 text-foreground">{selectedWorkflowInsight.lastRunOutcomeLabel}</div>
                  </div>
                  <div className="rounded-md border border-border/70 bg-muted/20 px-2 py-1.5 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">State</div>
                    <div className="mt-1 text-foreground">{selectedWorkflowInsight.stateLabel.replace(/_/g, " ")}</div>
                  </div>
                </div>

                <div className="rounded-md border border-border/70 bg-muted/20 px-3 py-2 text-xs">
                  <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Last Result Summary</div>
                  <div className="mt-1 leading-relaxed text-foreground">{selectedWorkflowInsight.lastResultSummary}</div>
                </div>

                <div className="rounded-md border border-warning/35 bg-warning/10 px-3 py-2 text-xs text-foreground">
                  <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Next Likely Action</div>
                  <div className="mt-1 leading-relaxed">{selectedWorkflowInsight.nextLikelyAction}</div>
                </div>

                {selectedWorkflowInsight.notificationBehaviorLabel ? (
                  <div className="rounded-md border border-border/70 bg-muted/20 px-3 py-2 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Notification Behavior</div>
                    <div className="mt-1 leading-relaxed text-foreground">{selectedWorkflowInsight.notificationBehaviorLabel}</div>
                  </div>
                ) : null}

                <div className="flex flex-wrap gap-2">
                  <Button
                    onClick={() =>
                      selectedPrimaryWorkflow ? runBlueprint(selectedPrimaryWorkflow.taskType, selectedPrimaryWorkflow.payload) : undefined
                    }
                    disabled={!selectedPrimaryWorkflow}
                  >
                    Run Workflow Now
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={() => {
                      if (selectedPrimaryWatcher) {
                        selectWatcherForEdit(selectedPrimaryWatcher);
                        return;
                      }
                      if (selectedPrimaryWorkflow) {
                        const currentInterval = primaryIntervals[selectedPrimaryWorkflow.id] || selectedPrimaryWorkflow.defaultInterval;
                        upsertPrimaryWatcher(selectedPrimaryWorkflow.id, currentInterval);
                      }
                    }}
                    disabled={!selectedPrimaryWorkflow}
                  >
                    {selectedPrimaryWatcher ? "Configure Workflow" : "Create Managed Watcher"}
                  </Button>
                  <Button variant="outline" onClick={() => navigate("/runs")}>
                    Open Runs
                  </Button>
                </div>

                {selectedWorkflowDetailId === "jobs" ? (
                  <div className="space-y-3 rounded-md border border-border/70 bg-muted/20 px-3 py-3">
                    <div>
                      <div className="text-sm font-semibold">Jobs Watcher Configuration</div>
                      <div className="text-xs text-muted-foreground">
                        Structured fields are grouped by pipeline stage: collection filters, ranking hints, and digest shortlist size.
                      </div>
                    </div>

                    <div className="grid gap-3 lg:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="jobs-desired-titles">Desired Titles (Collection + Ranking)</Label>
                        <Textarea
                          id="jobs-desired-titles"
                          className="min-h-[70px]"
                          value={jobsForm.desiredTitlesText}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, desiredTitlesText: event.target.value }));
                            setJobsFormError(null);
                          }}
                          placeholder="Machine Learning Engineer, AI Engineer"
                        />
                        <div className="text-[11px] text-muted-foreground">Comma or newline separated.</div>
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="jobs-locations">Preferred Locations (Collection + Ranking)</Label>
                        <Textarea
                          id="jobs-locations"
                          className="min-h-[70px]"
                          value={jobsForm.preferredLocationsText}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, preferredLocationsText: event.target.value }));
                            setJobsFormError(null);
                          }}
                          placeholder={"Remote\nNew York, NY"}
                        />
                        <div className="text-[11px] text-muted-foreground">One location per line.</div>
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="jobs-keywords">Keywords (Collection + Ranking)</Label>
                        <Textarea
                          id="jobs-keywords"
                          className="min-h-[70px]"
                          value={jobsForm.keywordsText}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, keywordsText: event.target.value }));
                            setJobsFormError(null);
                          }}
                          placeholder="python, llm, distributed systems"
                        />
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="jobs-excluded-keywords">Excluded Keywords (Collection + Ranking)</Label>
                        <Textarea
                          id="jobs-excluded-keywords"
                          className="min-h-[70px]"
                          value={jobsForm.excludedKeywordsText}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, excludedKeywordsText: event.target.value }));
                            setJobsFormError(null);
                          }}
                          placeholder="intern, unpaid"
                        />
                      </div>
                    </div>

                    <div className="grid gap-3 lg:grid-cols-3">
                      <div className="space-y-1">
                        <Label>Work Mode Preference (Collection + Ranking)</Label>
                        <div className="space-y-1 text-sm">
                          {JOB_WORK_MODE_OPTIONS.map((mode) => (
                            <label key={mode} className="flex items-center gap-2">
                              <input
                                type="checkbox"
                                checked={jobsForm.remotePreference[mode]}
                                onChange={(event) => setJobsWorkModeEnabled(mode, event.target.checked)}
                              />
                              <span className="capitalize">{mode}</span>
                            </label>
                          ))}
                        </div>
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="jobs-min-salary">Minimum Salary (Collection + Ranking)</Label>
                        <Input
                          id="jobs-min-salary"
                          value={jobsForm.minimumSalaryText}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, minimumSalaryText: event.target.value }));
                            setJobsFormError(null);
                          }}
                          placeholder="140000"
                        />
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="jobs-experience">Experience Level (Collection + Ranking)</Label>
                        <select
                          id="jobs-experience"
                          className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
                          value={jobsForm.experienceLevel}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, experienceLevel: event.target.value }));
                            setJobsFormError(null);
                          }}
                        >
                          {JOB_EXPERIENCE_LEVEL_OPTIONS.map((value) => (
                            <option key={value || "any"} value={value}>
                              {value ? value : "Any"}
                            </option>
                          ))}
                        </select>
                      </div>
                    </div>

                    <div className="grid gap-3 lg:grid-cols-3">
                      <div className="space-y-1">
                        <Label>Enabled Sources (Collection)</Label>
                        <div className="space-y-1 text-sm">
                          {JOB_SOURCE_OPTIONS.map((source) => (
                            <label key={source} className="flex items-center gap-2">
                              <input
                                type="checkbox"
                                checked={jobsForm.enabledSources[source]}
                                onChange={(event) => setJobsSourceEnabled(source, event.target.checked)}
                              />
                              <span className="capitalize">{source}</span>
                            </label>
                          ))}
                        </div>
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="jobs-limit-per-source">Result Limit Per Source (Collection)</Label>
                        <Input
                          id="jobs-limit-per-source"
                          type="number"
                          min={1}
                          max={100}
                          value={jobsForm.resultLimitPerSourceText}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, resultLimitPerSourceText: event.target.value }));
                            setJobsFormError(null);
                          }}
                        />
                      </div>

                      <div className="space-y-2">
                        <Label htmlFor="jobs-shortlist-count">Top-N Shortlist Count (Digest Size)</Label>
                        <Input
                          id="jobs-shortlist-count"
                          type="number"
                          min={1}
                          max={10}
                          value={jobsForm.shortlistCountText}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, shortlistCountText: event.target.value }));
                            setJobsFormError(null);
                          }}
                        />
                      </div>
                    </div>

                    <div className="grid gap-3 lg:grid-cols-3">
                      <div className="space-y-2">
                        <Label htmlFor="jobs-freshness-pref">Freshness Preference (Shortlist)</Label>
                        <select
                          id="jobs-freshness-pref"
                          className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
                          value={jobsForm.freshnessPreference}
                          onChange={(event) => {
                            setJobsForm((prev) => ({ ...prev, freshnessPreference: event.target.value }));
                            setJobsFormError(null);
                          }}
                        >
                          {JOB_FRESHNESS_PREFERENCE_OPTIONS.map((value) => (
                            <option key={value} value={value}>
                              {value.replace(/_/g, " ")}
                            </option>
                          ))}
                        </select>
                      </div>
                    </div>

                    {jobsFormError ? <div className="text-xs text-destructive">{jobsFormError}</div> : null}
                    <div className="flex flex-wrap gap-2">
                      <Button onClick={saveJobsWorkflowConfiguration}>Save Jobs Configuration</Button>
                      <Button
                        variant="outline"
                        onClick={() => {
                          setJobsForm(parseJobsWatcherFormFromPayload(selectedPrimaryPayloadJson));
                          setJobsFormError(null);
                        }}
                      >
                        Reset Form
                      </Button>
                    </div>
                  </div>
                ) : null}
              </>
            ) : (
              <EmptyState
                title="No workflow detail selected"
                description="Select a primary workflow card to inspect operational state, last outcome, and likely next action."
              />
            )}
          </CardContent>
        </Card>
      </section>

      <section>
        <SectionHeader
          title="Watcher Management"
          subtitle="Inventory first, then edit a selected watcher or create a new one."
        />
        <DataTableWrapper
          title="Watcher Inventory"
          subtitle={`${watchers.length} watchers total`}
          loading={watchersQuery.isLoading}
          error={watchersQuery.error ? errorMessage(watchersQuery.error) : null}
          onRetry={() => void watchersQuery.refetch()}
          isEmpty={!watchersQuery.isLoading && watchers.length === 0}
          emptyTitle="No watchers"
          emptyDescription="Create a watcher to persist automation payload, interval, and policy metadata."
        >
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Task Type</TableHead>
                <TableHead>Interval</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Last Run</TableHead>
                <TableHead>Last Result</TableHead>
                <TableHead>Next Action</TableHead>
                <TableHead>Updated</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {watchers.map((watcher) => {
                const insight = watcherInsightsById.get(watcher.id) || null;
                const primary = PRIMARY_WATCHERS.find((row) => row.watcherId === watcher.id || row.taskType === watcher.task_type);
                return (
                  <TableRow key={watcher.id} className={watcher.id === editWatcherId ? "bg-primary/10" : undefined}>
                    <TableCell>{watcher.name}</TableCell>
                    <TableCell>{watcher.task_type}</TableCell>
                    <TableCell>{insight?.effectiveIntervalLabel || `${watcher.interval_seconds}s`}</TableCell>
                    <TableCell><StatusBadge status={watcher.enabled ? "enabled" : "disabled"} /></TableCell>
                    <TableCell>
                      {watcher.last_run_summary ? (
                        <div className="space-y-1">
                          <StatusBadge status={watcher.last_run_summary.task_status} />
                          <div className="text-[11px] text-muted-foreground">{formatIso(watcher.last_run_summary.task_updated_at)}</div>
                        </div>
                      ) : (
                        "-"
                      )}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">{insight?.lastResultSummary || "No result summary yet."}</TableCell>
                    <TableCell className="text-xs text-muted-foreground">{insight?.nextLikelyAction || "-"}</TableCell>
                    <TableCell>{formatIso(watcher.updated_at)}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-2">
                        <Button size="sm" variant="outline" onClick={() => selectWatcherForEdit(watcher)}>
                          Edit
                        </Button>
                        {primary ? (
                          <Button size="sm" variant="secondary" onClick={() => setSelectedWorkflowDetailId(primary.id)}>
                            Details
                          </Button>
                        ) : null}
                        <Button
                          size="sm"
                          variant="secondary"
                          onClick={() => patchWatcherMutation.mutate({ watcherId: watcher.id, patch: { enabled: !watcher.enabled } })}
                        >
                          {watcher.enabled ? "Disable" : "Enable"}
                        </Button>
                        <Button size="sm" variant="destructive" onClick={() => deleteWatcherMutation.mutate(watcher.id)}>
                          Delete
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </DataTableWrapper>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Edit Selected Watcher</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {editWatcherId ? (
              <>
                <Input value={editName} onChange={(e) => { setEditName(e.target.value); setEditPayloadError(null); }} placeholder="Watcher name" />
                <div>
                  <Label>Watcher interval (seconds)</Label>
                  <Input
                    type="number"
                    value={editIntervalSec}
                    onChange={(e) => setEditIntervalSec(Math.max(60, Number(e.target.value) || 300))}
                  />
                </div>
                {editIsJobsWatcher ? (
                  <div className="rounded-md border border-border/70 bg-muted/20 px-3 py-2 text-xs">
                    <div className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">Jobs Watcher Editing</div>
                    <div className="mt-1 leading-relaxed text-foreground">
                      Use the Jobs Watcher Configuration section above for collection/ranking/digest fields. Raw payload editing is optional.
                    </div>
                    <details className="mt-2">
                      <summary className="cursor-pointer font-medium">Advanced Raw Payload</summary>
                      <Textarea
                        value={editPayload}
                        onChange={(e) => { setEditPayload(e.target.value); setEditPayloadError(null); }}
                        className="mt-2 min-h-[170px]"
                      />
                    </details>
                  </div>
                ) : (
                  <Textarea
                    value={editPayload}
                    onChange={(e) => { setEditPayload(e.target.value); setEditPayloadError(null); }}
                    className="min-h-[170px]"
                  />
                )}
                {editPayloadError ? <div className="text-xs text-destructive">{editPayloadError}</div> : null}
                <div className="flex flex-wrap gap-2">
                  <Button onClick={saveWatcherEdit}>Save Changes</Button>
                  <Button variant="secondary" onClick={() => { setEditWatcherId(null); setEditPayloadError(null); setEditIntervalSec(300); }}>
                    Clear Selection
                  </Button>
                </div>
              </>
            ) : (
              <EmptyState
                title="No watcher selected"
                description="Select an inventory row to edit payload, interval, and watcher policy metadata."
              />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Create New Watcher</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <Input
              value={createName}
              onChange={(e) => {
                setCreateName(e.target.value);
                setCreatePayloadError(null);
              }}
              placeholder="Watcher name"
            />
            <Input
              value={createTaskType}
              onChange={(e) => {
                setCreateTaskType(e.target.value);
                setCreatePayloadError(null);
              }}
              placeholder="task_type"
            />
            <div>
              <Label>Watcher interval (seconds)</Label>
              <Input
                type="number"
                value={createIntervalSec}
                onChange={(e) => setCreateIntervalSec(Math.max(60, Number(e.target.value) || 300))}
              />
            </div>
            <Textarea
              value={createPayload}
              onChange={(e) => {
                setCreatePayload(e.target.value);
                setCreatePayloadError(null);
              }}
              className="min-h-[170px]"
            />
            {createPayloadError ? <div className="text-xs text-destructive">{createPayloadError}</div> : null}
            <Button onClick={createWatcher}>Create Watcher</Button>
          </CardContent>
        </Card>
      </section>

      <section>
        <details className="rounded-lg border border-border/80 bg-card p-4">
          <summary className="cursor-pointer text-sm font-semibold tracking-tight">Advanced Planner Controls</summary>
          <div className="mt-4 space-y-4">
            <SectionHeader title="Planner Runtime Limits" subtitle="Demoted controls for per-cycle limits, approval policy, and reset." />
            <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
              <div>
                <Label>Max create / cycle</Label>
                <Input type="number" value={maxCreate} onChange={(e) => setMaxCreate(Math.max(0, Number(e.target.value) || 0))} />
              </div>
              <div>
                <Label>Max execute / cycle</Label>
                <Input type="number" value={maxExecute} onChange={(e) => setMaxExecute(Math.max(0, Number(e.target.value) || 0))} />
              </div>
              <div className="flex items-end">
                <Button
                  variant="outline"
                  onClick={() => patchConfigMutation.mutate({ max_create_per_cycle: maxCreate, max_execute_per_cycle: maxExecute })}
                >
                  Save Limits
                </Button>
              </div>
              <div className="flex items-end">
                <Button variant="destructive" onClick={() => resetConfigMutation.mutate()}>
                  Reset Planner Config
                </Button>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <Button variant="secondary" onClick={() => patchConfigMutation.mutate({ require_approval: !requiresApproval })}>
                {requiresApproval ? "Disable Approval Requirement" : "Require Approval"}
              </Button>
            </div>
          </div>
        </details>
      </section>
    </div>
  );
}
