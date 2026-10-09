import { useReducedMotion } from "~/hooks/useReducedMotion";
import { MorphIcon as BaseMorphIcon, type MorphIconProps } from "morphicons/react";

/** Renders `lucide` icon data and morphs between shapes when `icon` changes. */
export function MorphIcon({ reducedMotion = "user", ...props }: MorphIconProps) {
  const disabled = useReducedMotion();
  return <BaseMorphIcon reducedMotion={disabled ? "always" : reducedMotion} {...props} />;
}
