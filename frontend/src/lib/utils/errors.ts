import { ApiError } from "@/lib/api/client";

export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 408) {
      return `Request timed out${error.path ? ` for ${error.path}` : ""}.`;
    }
    return `${error.status} ${error.message}`;
  }
  if (error instanceof Error) return error.message;
  return String(error);
}
