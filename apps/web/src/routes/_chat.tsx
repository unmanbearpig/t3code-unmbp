import { AuthPreviewOperateScope } from "@t3tools/contracts";
import { Outlet, createFileRoute, redirect, useParams } from "@tanstack/react-router";
import { ThreadRouteView } from "../components/ThreadRouteView";
import { resolveThreadRouteTarget } from "../threadRoutes";
import { useClientSettings } from "../hooks/useSettings";

function ChatRouteLayout() {
  // Both thread routes render here, not in their own leaf components, so the
  // draft-to-thread promotion keeps one ChatView mounted across the swap.
  const threadTarget = useParams({
    strict: false,
    select: (params) => resolveThreadRouteTarget(params),
  });
  return <>{threadTarget ? <ThreadRouteView target={threadTarget} /> : <Outlet />}</>;
}

export const Route = createFileRoute("/_chat")({
  beforeLoad: async ({ context }) => {
    if (
      context.authGateState.status !== "authenticated" &&
      context.authGateState.status !== "hosted-static"
    ) {
      throw redirect({ to: "/pair", replace: true });
    }
  },
  component: ChatRouteLayout,
});
