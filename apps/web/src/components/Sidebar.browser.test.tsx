import { createMemoryHistory } from "@tanstack/react-router";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import type * as Context from "effect/Context";
import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import "~/index.css";

import { AppRoot } from "../AppRoot";
import { __setClientSettingsForTests, updateClientSettings } from "../hooks/useSettings";
import { getRouter } from "../router";
import {
  ENVIRONMENT_ID,
  installFakeEnvironment,
  threadWithHistory,
  type FakeDriver,
} from "../../test/fakeEnvironment";
import { act, render } from "../../test/render";

vi.mock("../../../../packages/client-runtime/src/connection/driver.ts", async (original) => {
  const [actual, { fakeDriver }, Layer] = await Promise.all([
    original<{ readonly ConnectionDriver: Context.Key<unknown, FakeDriver> }>(),
    import("../../test/fakeEnvironment"),
    import("effect/Layer"),
  ]);
  return { ...actual, layer: Layer.succeed(actual.ConnectionDriver, fakeDriver) };
});

vi.mock(import("../rpc/requestLatencyState"), async (original) => ({
  ...(await original()),
  trackRpcRequestSent: () => {},
}));

it("keeps a thread drag alive when the sidebar renders before and after activation", async () => {
  const thread = threadWithHistory("sidebar-drag", 1);
  using _environment = installFakeEnvironment([thread, threadWithHistory("other-thread", 1)]);
  __setClientSettingsForTests({
    ...DEFAULT_CLIENT_SETTINGS,
    onboardingCompletedAt: "2026-01-01T00:00:00.000Z",
    sidebarWorkingShelfEnabled: false,
    confirmThreadArchive: true,
  });
  await page.viewport(1400, 900);
  const router = getRouter(
    createMemoryHistory({ initialEntries: [`/${ENVIRONMENT_ID}/${thread.id}`] }),
  );
  await router.load();
  const container = document.createElement("div");
  container.style.cssText = "height: 900px; width: 1400px;";
  await using screen = await render(<AppRoot router={router} />, {
    container,
    // AppRoot supplies its own router.
    router: false,
  });
  const item = screen.getByRole("treeitem", { name: /Thread sidebar-drag/ });
  await expect.element(item).toBeVisible();
  const row = item.element().closest("li");
  if (row === null) throw new Error("Sidebar thread has no list row");
  const bounds = row.getBoundingClientRect();
  const pointer = (type: string, offset: number, buttons: number) =>
    new PointerEvent(type, {
      bubbles: true,
      cancelable: true,
      pointerId: 1,
      isPrimary: true,
      button: 0,
      buttons,
      clientX: bounds.left + bounds.width / 2,
      clientY: bounds.top + bounds.height / 2 + offset,
    });

  await act(() => row.dispatchEvent(pointer("pointerdown", 0, 1)));
  await act(() => updateClientSettings({ confirmThreadArchive: false }));
  await act(() => document.dispatchEvent(pointer("pointermove", 20, 1)));
  await expect.element(page.elementLocator(row)).toHaveClass("z-20");

  await act(() => updateClientSettings({ confirmThreadArchive: true }));
  await act(() => document.dispatchEvent(pointer("pointermove", 24, 1)));
  await expect.element(page.elementLocator(row)).toHaveClass("z-20");

  await act(() => document.dispatchEvent(pointer("pointerup", 24, 0)));
  await expect.element(page.elementLocator(row)).not.toHaveClass("z-20");
});
