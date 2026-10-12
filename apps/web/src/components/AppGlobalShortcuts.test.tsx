// @vitest-environment jsdom

import type {
  DesktopTerminalLaunchInput,
  DesktopTerminalLaunchResult,
  DesktopTerminalTarget,
  ResolvedKeybindingsConfig,
} from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";
import {
  compileResolvedKeybindingsConfig,
  DEFAULT_RESOLVED_KEYBINDINGS,
} from "@t3tools/shared/keybindings";
import { act } from "react";
import { create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { AppGlobalShortcuts } from "./AppGlobalShortcuts";

const state = vi.hoisted(() => ({
  keybindings: [] as ResolvedKeybindingsConfig,
  activeThread: {
    environmentId: "local",
    projectId: "project",
    worktreePath: "/project/worktree" as string | null,
  },
  presentation: {
    entry: {
      target: { _tag: "PrimaryConnectionTarget", httpBaseUrl: "http://127.0.0.1:3773" },
      profile: { _tag: "None" },
    },
  },
}));

vi.mock("@effect/atom-react", () => ({ useAtomValue: () => state.keybindings }));
vi.mock("../state/server", () => ({ primaryServerKeybindingsAtom: {} }));
vi.mock("../hooks/useHandleNewThread", () => ({
  useHandleNewThread: () => ({ activeThread: state.activeThread, activeDraftThread: null }),
}));
vi.mock("../hooks/useSettings", () => ({
  useClientSettings: (select: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    select(DEFAULT_CLIENT_SETTINGS),
  useLegacySidebarEnabled: () => false,
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
vi.mock("../state/environments", () => ({ usePrimaryEnvironmentId: () => "local" }));
vi.mock("../state/session", () => ({ useEnvironmentScope: () => false }));
vi.mock("../hooks/useScratchProject", () => ({ useScratchProject: () => ({}) }));
vi.mock("../logicalProject", () => ({ selectProjectGroupingSettings: () => ({}) }));
vi.mock("../sidebarProjectGrouping", () => ({ buildSidebarProjectSnapshots: () => [] }));
vi.mock("../lib/chatThreadActions", () => ({ startNewThreadFromContext: vi.fn() }));
vi.mock("../hooks/showThreadUndoNotice", () => ({ undoLatestThreadAction: vi.fn() }));
vi.mock("../browser/previewRuntime", () => ({ isPreviewAvailableFor: () => false }));
vi.mock("../terminalUiStateStore", () => ({
  useTerminalUiStateStore: () => false,
  selectThreadTerminalUiState: () => ({ terminalOpen: false }),
}));
vi.mock("../rightPanelStore", () => ({
  useRightPanelStore: () => false,
  selectActiveRightPanel: () => null,
}));
vi.mock("./ui/toast", () => ({ stackedThreadToast: vi.fn(), toastManager: { add: vi.fn() } }));

const probe = vi.fn<(target: DesktopTerminalTarget) => Promise<boolean>>();
const launch = vi.fn<(input: DesktopTerminalLaunchInput) => Promise<DesktopTerminalLaunchResult>>();
let renderer: ReactTestRenderer | undefined;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal("navigator", { platform: "Linux" });
  state.keybindings = DEFAULT_RESOLVED_KEYBINDINGS;
  state.activeThread = {
    environmentId: "local",
    projectId: "project",
    worktreePath: "/project/worktree",
  };
  probe.mockReset().mockResolvedValue(true);
  launch.mockReset().mockResolvedValue({ opened: true });
  Object.defineProperty(window, "desktopBridge", {
    configurable: true,
    value: { isLocalTerminalEnvironment: probe, openInTerminal: launch },
  });
});

afterEach(async () => {
  await act(() => renderer?.unmount());
  renderer = undefined;
  document.body.replaceChildren();
  delete window.desktopBridge;
  vi.unstubAllGlobals();
});

async function mount() {
  await act(async () => {
    renderer = create(<AppGlobalShortcuts />);
  });
}

function press(overrides: KeyboardEventInit = {}, target: EventTarget = window) {
  const event = new KeyboardEvent("keydown", {
    key: "N",
    code: "KeyN",
    ctrlKey: true,
    shiftKey: true,
    bubbles: true,
    cancelable: true,
    ...overrides,
  });
  target.dispatchEvent(event);
  return event;
}

it("launches the active worktree through desktop IPC when Ctrl+Shift+N is pressed", async () => {
  await mount();
  expect(press().defaultPrevented).toBe(true);
  expect(launch).toHaveBeenCalledExactlyOnceWith({
    environmentId: "local",
    candidateUrls: ["http://127.0.0.1:3773"],
    cwd: "/project/worktree",
    terminal: "foot",
  });
});

it("uses the project directory when the thread has no worktree", async () => {
  state.activeThread.worktreePath = null;
  await mount();
  press();
  expect(launch).toHaveBeenCalledWith(expect.objectContaining({ cwd: "/project" }));
});

it("opens one window per deliberate press and ignores held-key repeats", async () => {
  await mount();
  press();
  expect(press({ repeat: true }).defaultPrevented).toBe(true);
  expect(launch).toHaveBeenCalledTimes(1);
  press();
  expect(launch).toHaveBeenCalledTimes(2);
});

it("honors a customized shortcut and releases Ctrl+Shift+N", async () => {
  state.keybindings = compileResolvedKeybindingsConfig([
    { key: "ctrl+alt+t", command: "terminal.openExternal", when: "externalTerminalAvailable" },
  ]);
  await mount();
  expect(press().defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
  expect(press({ key: "t", code: "KeyT", shiftKey: false, altKey: true }).defaultPrevented).toBe(
    true,
  );
  expect(launch).toHaveBeenCalledTimes(1);
});

it("leaves the chord with the focused terminal", async () => {
  await mount();
  const terminal = document.createElement("textarea");
  terminal.dataset.terminalOwner = "drawer";
  document.body.append(terminal);
  terminal.focus();
  expect(press({}, terminal).defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not launch while the command palette or shortcut recorder owns input", async () => {
  await mount();
  const palette = document.createElement("div");
  palette.dataset.commandPalette = "";
  document.body.append(palette);
  expect(press().defaultPrevented).toBe(false);
  palette.remove();
  const recorder = document.createElement("input");
  recorder.dataset.keybindingCapture = "";
  document.body.append(recorder);
  expect(press({}, recorder).defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not consume the shortcut for an environment on another computer", async () => {
  state.activeThread.environmentId = "remote";
  probe.mockResolvedValue(false);
  await mount();
  expect(press().defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not launch an unavailable terminal even with an unconditional custom binding", async () => {
  state.keybindings = compileResolvedKeybindingsConfig([
    { key: "ctrl+shift+n", command: "terminal.openExternal" },
  ]);
  probe.mockResolvedValue(false);
  await mount();
  expect(press().defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not consume the shortcut in the web client", async () => {
  delete window.desktopBridge;
  await mount();
  expect(press().defaultPrevented).toBe(false);
  expect(probe).not.toHaveBeenCalled();
  expect(launch).not.toHaveBeenCalled();
});
