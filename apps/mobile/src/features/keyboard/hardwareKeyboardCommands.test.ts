import { mobileLeaderCommand } from "./hardwareKeyboardCommands";
import { describe, expect, it } from "vite-plus/test";

import { parseActiveThreadPath } from "./hardwareKeyboardCommands";

describe("parseActiveThreadPath", () => {
  it("decodes route components", () => {
    expect(parseActiveThreadPath("/threads/local%20machine/thread%2Fone/review")).toEqual({
      environmentId: "local machine",
      threadId: "thread/one",
    });
  });

  it("ignores malformed encoded route components", () => {
    expect(parseActiveThreadPath("/threads/%E0%A4%A/thread-1")).toBeNull();
  });
});

describe("mobile leader commands", () => {
  it("maps app commands to native actions and leaves unavailable commands out", () => {
    expect(mobileLeaderCommand("thread.next")).toBe("thread.next");
    expect(mobileLeaderCommand("thread.jump.1")).toBe("thread.jump.1");
    expect(mobileLeaderCommand("terminal.toggle")).toBe("terminal");
    expect(mobileLeaderCommand("commandPalette.toggle")).toBe("commandPalette");
    expect(mobileLeaderCommand("modelPicker.toggle")).toBeNull();
  });
});
