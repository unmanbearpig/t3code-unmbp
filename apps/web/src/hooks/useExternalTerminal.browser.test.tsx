import type {
  DesktopTerminalLaunchInput,
  DesktopTerminalLaunchResult,
  DesktopTerminalTarget,
} from "@t3tools/contracts";
import { useLayoutEffect } from "react";
import { act, render } from "../../test/render";
import { expect, it, vi } from "vite-plus/test";

import { useExternalTerminal } from "./useExternalTerminal";

const state = vi.hoisted(() => ({
  activeThread: { environmentId: "local", projectId: "project", worktreePath: "/project/worktree" },
  presentation: {
    entry: {
      target: {
        _tag: "BearerConnectionTarget",
        environmentId: "local",
        connectionId: "saved",
        label: "Local",
      },
      profile: {
        _tag: "Some",
        value: { _tag: "BearerConnectionProfile", httpBaseUrl: "http://127.0.0.1:3773" },
      },
    },
    serverConfig: undefined,
  },
  toast: vi.fn(),
}));

vi.mock("./useHandleNewThread", () => ({
  useHandleNewThread: () => ({ activeThread: state.activeThread, activeDraftThread: null }),
}));
vi.mock("../state/entities", () => ({
  useProjects: () => [
    { environmentId: "local", id: "project", workspaceRoot: "/project" },
    { environmentId: "remote", id: "project", workspaceRoot: "/project" },
  ],
}));
vi.mock("../state/presentation", () => ({
  useEnvironmentPresentation: () => ({ presentation: state.presentation }),
}));
vi.mock("./useSettings", () => ({ useClientSettings: () => "foot" }));
vi.mock("../components/ui/toast", () => ({
  stackedThreadToast: (value: unknown) => value,
  toastManager: { add: state.toast },
}));

function Probe({
  onChange,
}: {
  onChange: (value: ReturnType<typeof useExternalTerminal>) => void;
}) {
  const terminal = useExternalTerminal();
  useLayoutEffect(() => {
    onChange(terminal);
  });
  return null;
}

const probe = vi.fn<(target: DesktopTerminalTarget) => Promise<boolean>>();
const launch = vi.fn<(input: DesktopTerminalLaunchInput) => Promise<DesktopTerminalLaunchResult>>();

function setup() {
  const previous = Object.getOwnPropertyDescriptor(window, "desktopBridge");
  Object.defineProperty(window, "desktopBridge", {
    configurable: true,
    value: { isLocalTerminalEnvironment: probe, openInTerminal: launch },
  });
  probe.mockReset().mockResolvedValue(true);
  launch.mockReset().mockResolvedValue({ opened: true });
  state.toast.mockClear();
  state.activeThread = {
    environmentId: "local",
    projectId: "project",
    worktreePath: "/project/worktree",
  };
  state.presentation.entry.profile.value.httpBaseUrl = "http://127.0.0.1:3773";
  let value: ReturnType<typeof useExternalTerminal>;
  return {
    onChange: (next: ReturnType<typeof useExternalTerminal>) => {
      value = next;
    },
    get value() {
      return value;
    },
    [Symbol.dispose]() {
      if (previous) Object.defineProperty(window, "desktopBridge", previous);
      else delete window.desktopBridge;
    },
  };
}

it("opens the active worktree through the desktop without a server capability flag", async () => {
  using context = setup();
  await using _screen = await render(<Probe onChange={context.onChange} />);
  expect(context.value.available).toBe(true);
  await context.value.open();
  expect(launch).toHaveBeenCalledWith({
    environmentId: "local",
    candidateUrls: ["http://127.0.0.1:3773"],
    cwd: "/project/worktree",
    terminal: "foot",
  });
});

it("does not launch for a remote thread with the same project path", async () => {
  using context = setup();
  probe.mockResolvedValue(false);
  state.activeThread.environmentId = "remote";
  await using _screen = await render(<Probe onChange={context.onChange} />);
  expect(context.value.available).toBe(false);
  await context.value.open();
  expect(launch).not.toHaveBeenCalled();
});

it("stops using the previous environment while a new locality check is pending", async () => {
  using context = setup();
  await using screen = await render(<Probe onChange={context.onChange} />);
  let finishProbe: (local: boolean) => void = () => undefined;
  probe.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finishProbe = resolve;
      }),
  );
  state.activeThread = { ...state.activeThread, environmentId: "remote" };
  await act(async () => {
    await screen.rerender(<Probe onChange={context.onChange} />);
  });
  expect(context.value.available).toBe(false);
  await context.value.open();
  expect(launch).not.toHaveBeenCalled();
  await act(async () => {
    finishProbe(false);
  });
  expect(context.value.available).toBe(false);
});

it("ignores a late local result after switching to a remote thread", async () => {
  using context = setup();
  let finishProbe: (local: boolean) => void = () => undefined;
  probe.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finishProbe = resolve;
      }),
  );
  await using screen = await render(<Probe onChange={context.onChange} />);
  state.activeThread = { ...state.activeThread, environmentId: "remote" };
  probe.mockResolvedValue(false);
  await act(async () => {
    await screen.rerender(<Probe onChange={context.onChange} />);
  });
  await act(async () => {
    finishProbe(true);
  });
  expect(context.value.available).toBe(false);
  await context.value.open();
  expect(launch).not.toHaveBeenCalled();
});

it("does not launch in a browser or an older desktop shell", async () => {
  using context = setup();
  delete window.desktopBridge;
  await using _screen = await render(<Probe onChange={context.onChange} />);
  expect(context.value.available).toBe(false);
  await context.value.open();
  expect(probe).not.toHaveBeenCalled();
  expect(launch).not.toHaveBeenCalled();
});

it("shows an error from the desktop terminal launcher", async () => {
  using context = setup();
  launch.mockResolvedValue({
    opened: false,
    error: "foot is not installed or is not available on PATH.",
  });
  await using _screen = await render(<Probe onChange={context.onChange} />);
  await context.value.open();
  expect(state.toast).toHaveBeenCalledWith(
    expect.objectContaining({
      type: "error",
      description: "foot is not installed or is not available on PATH.",
    }),
  );
});
