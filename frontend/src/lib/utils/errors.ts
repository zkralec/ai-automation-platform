import { ApiError } from "@/lib/api/client";

export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 0) {
      return "Browser could not reach the Mission Control API. This does not by itself mean server-side tasks failed.";
    }
    if (error.status === 408) {
      return `Request timed out${error.path ? ` for ${error.path}` : ""}.`;
    }
    return `${error.status} ${error.message}`;
  }
  if (error instanceof Error) {
    const message = error.message.trim();
    const normalized = message.toLowerCase();
    if (normalized.includes("failed to fetch") || normalized.includes("networkerror")) {
      return "Browser could not reach the Mission Control API. This does not by itself mean server-side tasks failed.";
    }
    return message;
  }
  return String(error);
}
