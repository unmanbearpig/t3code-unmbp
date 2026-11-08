import { useClientSettings } from "./useSettings";
import { useMediaQuery } from "./useMediaQuery";

export function useReducedMotion(): boolean {
  const disabled = useClientSettings((settings) => settings.disableAnimations);
  const systemReducedMotion = useMediaQuery("(prefers-reduced-motion: reduce)");
  return disabled || systemReducedMotion;
}
