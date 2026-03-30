import { Badge, type BadgeProps } from "@/components/ui/badge";

type StatusBadgeProps = {
  status?: string | null;
};

const statusVariantMap: Record<string, BadgeProps["variant"]> = {
  live: "success",
  stale: "destructive",
  historical: "secondary",
  offline: "secondary",
  success: "success",
  succeeded: "success",
  ok: "success",
  true: "success",
  present: "success",
  enabled: "success",
  running: "warning",
  execution: "success",
  execute: "success",
  recommendation: "secondary",
  paused: "outline",
  queued: "secondary",
  manual: "secondary",
  ready: "success",
  missing: "warning",
  awaiting: "warning",
  not_required: "outline",
  retryable: "warning",
  permanent: "destructive",
  failed: "destructive",
  failed_permanent: "destructive",
  blocked_budget: "destructive",
  error: "destructive",
  false: "destructive",
  not_ready: "destructive",
  disabled: "outline",
  warning: "warning",
  info: "secondary"
};

export function resolveStatusVariant(status?: string | null): BadgeProps["variant"] {
  const normalized = (status || "unknown").toLowerCase();
  return statusVariantMap[normalized] || "outline";
}

export function StatusBadge({ status }: StatusBadgeProps): JSX.Element {
  const normalized = (status || "unknown").toLowerCase();
  const label = normalized.replace(/_/g, " ");
  return <Badge variant={resolveStatusVariant(normalized)}>{label}</Badge>;
}
