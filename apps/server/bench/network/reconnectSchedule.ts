import { variantOrder, type ReconnectCase } from "./reconnectManifest.ts";

const hash = (text: string) =>
  [...text].reduce((n, c) => (Math.imul(n, 31) + c.charCodeAt(0)) >>> 0, 17);
export function scheduleLanes<T>(
  cases: readonly ReconnectCase[],
  variants: readonly T[],
  lanes: number,
) {
  if (!Number.isSafeInteger(lanes) || lanes < 1 || lanes > 18)
    throw new Error("Lanes must be 1..18");
  if (!variants.length) throw new Error("At least one variant is required");
  const result: { entry: ReconnectCase; variants: readonly T[] }[][] = Array.from(
    { length: lanes },
    () => [],
  );
  // Match all variants on the same lane. Adjacent seeds rotate lanes, and cell order is
  // reproducibly shuffled so runtime drift cannot track profile or variant ordering.
  for (const entry of [...cases].sort(
    (a, b) => hash(a.id) - hash(b.id) || a.id.localeCompare(b.id),
  )) {
    const cell = [
      entry.profile.name,
      entry.workload,
      entry.scheme,
      entry.routes,
      entry.phaseMs,
      entry.configDelayMs ?? 0,
      entry.slowSocketDelayMs ?? 0,
    ].join("/");
    result[(hash(cell) + entry.repetition - 1) % lanes]!.push({
      entry,
      variants: variantOrder(variants, entry),
    });
  }
  return result;
}
