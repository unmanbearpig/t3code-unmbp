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
import { render } from "../../test/render";
import { expect, it, vi } from "vite-plus/test";

import { AppGlobalShortcuts } from "./AppGlobalShortcuts";

const state = vi.hoisted(() => ({
  keybindings: [] as ResolvedKeybindingsConfig,
  paletteOpen: false,
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

vi.mock("../commandPaletteBus", () => ({
  isCommandPaletteOpen: () => state.paletteOpen,
  openCommandPalette: vi.fn(),
}));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => state.keybindings }));
vi.mock("../state/server", () => ({ primaryServerKeybindingsAtom: {} }));
vi.mock("../hooks/useHandleNewThread", () => ({
  useHandleNewThread: () => ({ activeThread: state.activeThread, activeDraftThread: null }),
}));
vi.mock("../hooks/useSettings", () => ({
  useClientSettings: (select: (settings: typeof DEFAULT_CLIENT_SETTINGS) => unknown) =>
    select(DEFAULT_CLIENT_SETTINGS),
  useBoringSidebarEnabled: () => false,
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
vi.mock("@t3tools/client-runtime/state/project-grouping", () => ({
  selectProjectGroupingSettings: () => ({}),
}));
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
  selectThreadRightPanelState: () => ({}),
}));
vi.mock("./ui/toast", () => ({ stackedThreadToast: vi.fn(), toastManager: { add: vi.fn() } }));

const probe = vi.fn<(target: DesktopTerminalTarget) => Promise<boolean>>();
const launch = vi.fn<(input: DesktopTerminalLaunchInput) => Promise<DesktopTerminalLaunchResult>>();
function setup() {
  const previous = Object.getOwnPropertyDescriptor(window, "desktopBridge");
  const platform = vi.spyOn(navigator, "platform", "get").mockReturnValue("Linux");
  state.keybindings = DEFAULT_RESOLVED_KEYBINDINGS;
  state.paletteOpen = false;
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
  return {
    [Symbol.dispose]() {
      platform.mockRestore();
      if (previous) Object.defineProperty(window, "desktopBridge", previous);
      else delete window.desktopBridge;
    },
  };
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
  using _context = setup();
  await using _screen = await render(<AppGlobalShortcuts />);
  expect(press().defaultPrevented).toBe(true);
  expect(launch).toHaveBeenCalledExactlyOnceWith({
    environmentId: "local",
    candidateUrls: ["http://127.0.0.1:3773"],
    cwd: "/project/worktree",
    terminal: "foot",
  });
});

it("uses the project directory when the thread has no worktree", async () => {
  using _context = setup();
  state.activeThread.worktreePath = null;
  await using _screen = await render(<AppGlobalShortcuts />);
  press();
  expect(launch).toHaveBeenCalledWith(expect.objectContaining({ cwd: "/project" }));
});

it("opens one window per deliberate press and ignores held-key repeats", async () => {
  using _context = setup();
  await using _screen = await render(<AppGlobalShortcuts />);
  press();
  expect(press({ repeat: true }).defaultPrevented).toBe(true);
  expect(launch).toHaveBeenCalledTimes(1);
  press();
  expect(launch).toHaveBeenCalledTimes(2);
});

it("honors a customized shortcut and releases Ctrl+Shift+N", async () => {
  using _context = setup();
  state.keybindings = compileResolvedKeybindingsConfig([
    { key: "ctrl+alt+t", command: "terminal.openExternal", when: "externalTerminalAvailable" },
  ]);
  await using _screen = await render(<AppGlobalShortcuts />);
  expect(press().defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
  expect(press({ key: "t", code: "KeyT", shiftKey: false, altKey: true }).defaultPrevented).toBe(
    true,
  );
  expect(launch).toHaveBeenCalledTimes(1);
});

it("leaves the chord with the focused terminal", async () => {
  using _context = setup();
  await using screen = await render(<AppGlobalShortcuts />);
  const terminal = document.createElement("textarea");
  terminal.dataset.terminalOwner = "drawer";
  screen.container.append(terminal);
  terminal.focus();
  expect(press({}, terminal).defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not launch while the command palette or shortcut recorder owns input", async () => {
  using _context = setup();
  await using screen = await render(<AppGlobalShortcuts />);
  const palette = document.createElement("div");
  palette.dataset.commandPalette = "";
  screen.container.append(palette);
  state.paletteOpen = true;
  expect(press().defaultPrevented).toBe(false);
  palette.remove();
  state.paletteOpen = false;
  const recorder = document.createElement("input");
  recorder.dataset.keybindingCapture = "";
  screen.container.append(recorder);
  expect(press({}, recorder).defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not consume the shortcut for an environment on another computer", async () => {
  using _context = setup();
  state.activeThread.environmentId = "remote";
  probe.mockResolvedValue(false);
  await using _screen = await render(<AppGlobalShortcuts />);
  expect(press().defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not launch an unavailable terminal even with an unconditional custom binding", async () => {
  using _context = setup();
  state.keybindings = compileResolvedKeybindingsConfig([
    { key: "ctrl+shift+n", command: "terminal.openExternal" },
  ]);
  probe.mockResolvedValue(false);
  await using _screen = await render(<AppGlobalShortcuts />);
  expect(press().defaultPrevented).toBe(false);
  expect(launch).not.toHaveBeenCalled();
});

it("does not consume the shortcut in the web client", async () => {
  using _context = setup();
  delete window.desktopBridge;
  await using _screen = await render(<AppGlobalShortcuts />);
  expect(press().defaultPrevented).toBe(false);
  expect(probe).not.toHaveBeenCalled();
  expect(launch).not.toHaveBeenCalled();
});
