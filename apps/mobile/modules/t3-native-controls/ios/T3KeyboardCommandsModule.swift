import ExpoModulesCore
import UIKit

public final class T3KeyboardCommandsModule: Module {
  public func definition() -> ModuleDefinition {
    Name("T3KeyboardCommands")

    View(T3KeyboardCommandsView.self) {
      Prop("enabledCommands") { (view: T3KeyboardCommandsView, commands: [String]) in
        view.setEnabledCommands(commands)
      }
      Prop("leaderConfig") { (view: T3KeyboardCommandsView, config: [String: Any]?) in
        view.setLeaderConfig(config)
      }
      Events("onCommand")
    }
  }
}

public final class T3KeyboardCommandsView: ExpoView {
  let onCommand = EventDispatcher()
  private var enabledCommands = Set<String>()
  private var leaderConfig: [String: Any]?
  private var leaderDeadline: TimeInterval = 0
  private var leaderTimer: Timer?

  func setLeaderConfig(_ config: [String: Any]?) {
    cancelLeader()
    leaderConfig = config
  }

  @objc private func cancelLeader() {
    leaderDeadline = 0
    leaderTimer?.invalidate()
    leaderTimer = nil
  }

  private func leaderInput(_ key: String) -> String {
    switch key {
    case "escape": return UIKeyCommand.inputEscape
    case "enter": return "\r"
    case "tab": return "\t"
    case "arrowup": return UIKeyCommand.inputUpArrow
    case "arrowdown": return UIKeyCommand.inputDownArrow
    case "arrowleft": return UIKeyCommand.inputLeftArrow
    case "arrowright": return UIKeyCommand.inputRightArrow
    default: return key
    }
  }

  private func leaderModifiers(_ stroke: [String: Any]) -> UIKeyModifierFlags {
    var modifiers: UIKeyModifierFlags = []
    if stroke["ctrlKey"] as? Bool == true { modifiers.insert(.control) }
    if stroke["metaKey"] as? Bool == true { modifiers.insert(.command) }
    if stroke["altKey"] as? Bool == true { modifiers.insert(.alternate) }
    if stroke["shiftKey"] as? Bool == true { modifiers.insert(.shift) }
    return modifiers
  }

  @objc private func armLeader() {
    if let input = window?.t3FirstResponder as? UITextInput, input.markedTextRange != nil { return }
    leaderDeadline = ProcessInfo.processInfo.systemUptime + 2
    leaderTimer?.invalidate()
    leaderTimer = Timer.scheduledTimer(withTimeInterval: 2, repeats: false) { [weak self] _ in self?.cancelLeader() }
  }

  @objc private func leaderStroke(_ sender: UIKeyCommand) {
    guard leaderDeadline > ProcessInfo.processInfo.systemUptime else { cancelLeader(); return }
    let bindings = leaderConfig?["bindings"] as? [[String: Any]] ?? []
    let binding = bindings.first { stroke in
      guard let key = stroke["key"] as? String else { return false }
      return leaderInput(key) == sender.input && leaderModifiers(stroke) == sender.modifierFlags
    }
    cancelLeader()
    if let identifier = binding?["command"] as? String, enabledCommands.contains(identifier) { emit(identifier) }
  }

  public override func pressesBegan(_ presses: Set<UIPress>, with event: UIPressesEvent?) {
    if leaderDeadline > 0 && presses.contains(where: { $0.key?.characters.isEmpty == false }) { cancelLeader() }
    super.pressesBegan(presses, with: event)
  }

  public override var canBecomeFirstResponder: Bool { true }

  public override func canPerformAction(_ action: Selector, withSender sender: Any?) -> Bool {
    if action == #selector(armLeader) || action == #selector(leaderStroke(_:)) || action == #selector(openCommandPalette) || action == #selector(paletteNext) || action == #selector(palettePrevious) || action == #selector(paletteDismiss),
       let input = window?.t3FirstResponder as? UITextInput,
       input.markedTextRange != nil {
      return false
    }
    return super.canPerformAction(action, withSender: sender)
  }

  public override var keyCommands: [UIKeyCommand]? {
    let isPad = UIDevice.current.userInterfaceIdiom == .pad
    var commands = [
      enabledCommand("newTask", input: "n", modifiers: .command, action: #selector(newTask), title: "New Task"),
      enabledCommand("focusSearch", input: "f", modifiers: .command, action: #selector(focusSearch), title: "Find"),
      isPad
        ? enabledCommand("commandPalette", input: "k", modifiers: .command, action: #selector(openCommandPalette), title: "Command Palette")
        : enabledCommand("focusSearch", input: "k", modifiers: .command, action: #selector(focusSearch), title: "Focus Search"),
      enabledCommand("paletteNext", input: UIKeyCommand.inputDownArrow, modifiers: [], action: #selector(paletteNext), title: "Next Result"),
      enabledCommand("palettePrevious", input: UIKeyCommand.inputUpArrow, modifiers: [], action: #selector(palettePrevious), title: "Previous Result"),
      enabledCommand("paletteDismiss", input: UIKeyCommand.inputEscape, modifiers: [], action: #selector(paletteDismiss), title: "Close Command Palette"),
      enabledCommand("back", input: "[", modifiers: .command, action: #selector(goBack), title: "Back"),
      enabledCommand("files", input: "f", modifiers: [.command, .shift], action: #selector(openFiles), title: "Open Files"),
      enabledCommand("terminal", input: "t", modifiers: [.command, .shift], action: #selector(openTerminal), title: "Open Terminal"),
      enabledCommand("review", input: "r", modifiers: [.command, .shift], action: #selector(openReview), title: "Open Review"),
      enabledCommand(
        "copyThreadReference",
        input: "c",
        modifiers: [.command, .shift],
        action: #selector(copyThreadReference),
        title: "Copy PR Link or Thread ID"
      ),
      enabledCommand("toggleSidebar", input: "\\", modifiers: .command, action: #selector(handleToggleSidebar), title: "Toggle Sidebar"),
      enabledCommand("cycleHost", input: "h", modifiers: [.command, .shift], action: #selector(cycleHost), title: "Next Machine"),
    ].compactMap { $0 }
    if isPad {
      commands += (1...9).compactMap { index in
        enabledCommand(
          "thread.jump.\(index)",
          input: String(index),
          modifiers: .command,
          action: #selector(jumpToThread(_:)),
          title: "Go to Thread \(index)"
        )
      }
    }
    if let trigger = leaderConfig?["trigger"] as? [String: Any], let key = trigger["key"] as? String {
      commands.append(command(leaderInput(key), modifiers: leaderModifiers(trigger), action: #selector(armLeader), title: "Leader"))
    }
    if leaderDeadline > ProcessInfo.processInfo.systemUptime {
      commands.removeAll { $0.input == UIKeyCommand.inputEscape && $0.modifierFlags.isEmpty }
      let bindings = leaderConfig?["bindings"] as? [[String: Any]] ?? []
      commands += bindings.compactMap { stroke in
        guard let key = stroke["key"] as? String, let identifier = stroke["command"] as? String,
          enabledCommands.contains(identifier) else { return nil }
        return command(leaderInput(key), modifiers: leaderModifiers(stroke), action: #selector(leaderStroke(_:)), title: identifier)
      }
      commands.append(command(UIKeyCommand.inputEscape, modifiers: [], action: #selector(cancelLeader), title: "Cancel Leader"))
    }
    return commands
  }

  func setEnabledCommands(_ commands: [String]) {
    enabledCommands = Set(commands)
    if isFirstResponder {
      resignFirstResponder()
    }
    reclaimFirstResponderIfAvailable()
  }

  private func enabledCommand(
    _ identifier: String,
    input: String,
    modifiers: UIKeyModifierFlags,
    action: Selector,
    title: String
  ) -> UIKeyCommand? {
    guard enabledCommands.contains(identifier) else { return nil }
    return command(input, modifiers: modifiers, action: action, title: title)
  }

  public required init(appContext: AppContext? = nil) {
    super.init(appContext: appContext)
    for notification in [UITextView.textDidChangeNotification, UITextField.textDidChangeNotification,
      UITextView.textDidBeginEditingNotification, UITextField.textDidBeginEditingNotification,
      UIApplication.willResignActiveNotification] {
      NotificationCenter.default.addObserver(self, selector: #selector(cancelLeader), name: notification, object: nil)
    }
    NotificationCenter.default.addObserver(
      self,
      selector: #selector(reclaimFirstResponderIfAvailable),
      name: UITextView.textDidEndEditingNotification,
      object: nil
    )
    NotificationCenter.default.addObserver(
      self,
      selector: #selector(reclaimFirstResponderIfAvailable),
      name: UITextField.textDidEndEditingNotification,
      object: nil
    )
    NotificationCenter.default.addObserver(
      self,
      selector: #selector(reclaimFirstResponderIfAvailable),
      name: UIApplication.didBecomeActiveNotification,
      object: nil
    )
  }

  deinit {
    leaderTimer?.invalidate()
    NotificationCenter.default.removeObserver(self)
  }

  public override func didMoveToWindow() {
    super.didMoveToWindow()
    if window == nil { cancelLeader() }
    reclaimFirstResponderIfAvailable()
  }

  public override func layoutSubviews() {
    super.layoutSubviews()
    reclaimFirstResponderIfAvailable()
  }

  private func command(
    _ input: String,
    modifiers: UIKeyModifierFlags,
    action: Selector,
    title: String
  ) -> UIKeyCommand {
    let command = UIKeyCommand(input: input, modifierFlags: modifiers, action: action)
    command.discoverabilityTitle = title
    command.wantsPriorityOverSystemBehavior = true
    return command
  }

  @objc private func newTask() { emit("newTask") }
  @objc private func openCommandPalette() { emit("commandPalette") }
  @objc private func paletteNext() { emit("paletteNext") }
  @objc private func palettePrevious() { emit("palettePrevious") }
  @objc private func paletteDismiss() { emit("paletteDismiss") }
  @objc private func jumpToThread(_ sender: UIKeyCommand) {
    guard let input = sender.input else { return }
    emit("thread.jump.\(input)")
  }
  @objc private func focusSearch() { emit("focusSearch") }
  @objc private func goBack() { emit("back") }
  @objc private func openFiles() { emit("files") }
  @objc private func openTerminal() { emit("terminal") }
  @objc private func openReview() { emit("review") }
  @objc private func copyThreadReference() { emit("copyThreadReference") }
  @objc private func handleToggleSidebar() { emit("toggleSidebar") }
  @objc private func cycleHost() { emit("cycleHost") }

  private func emit(_ command: String) {
    cancelLeader()
    onCommand(["command": command])
  }

  @objc private func reclaimFirstResponderIfAvailable() {
    DispatchQueue.main.async { [weak self] in
      guard let self, self.window?.t3FirstResponder == nil else { return }
      self.becomeFirstResponder()
    }
  }
}

private extension UIView {
  var t3FirstResponder: UIResponder? {
    if isFirstResponder { return self }
    for subview in subviews {
      if let responder = subview.t3FirstResponder { return responder }
    }
    return nil
  }
}
