package expo.modules.t3nativecontrols

import android.content.Context
import android.os.SystemClock
import android.view.KeyEvent
import expo.modules.kotlin.AppContext
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import expo.modules.kotlin.viewevent.EventDispatcher
import expo.modules.kotlin.views.ExpoView

class T3KeyboardCommandsModule : Module() {
  override fun definition() = ModuleDefinition {
    Name("T3KeyboardCommands")
    View(T3KeyboardCommandsView::class) {
      Prop("enabledCommands") { view: T3KeyboardCommandsView, commands: List<String> ->
        view.enabledCommands = commands.toSet()
      }
      Prop("leaderConfig") { view: T3KeyboardCommandsView, config: Map<String, Any?>? ->
        view.setLeaderConfig(config)
      }
      Events("onCommand")
    }
  }
}

class T3KeyboardCommandsView(context: Context, appContext: AppContext) : ExpoView(context, appContext) {
  private val onCommand by EventDispatcher()
  var enabledCommands = emptySet<String>()
  private var leaderConfig: Map<String, Any?>? = null
  private var leaderDeadline = 0L
  private val suppressedKeys = mutableSetOf<Int>()

  fun setLeaderConfig(config: Map<String, Any?>?) {
    leaderConfig = config
    leaderDeadline = 0L
  }

  override fun onWindowFocusChanged(hasWindowFocus: Boolean) {
    if (!hasWindowFocus) leaderDeadline = 0L
    super.onWindowFocusChanged(hasWindowFocus)
  }

  override fun dispatchKeyEvent(event: KeyEvent): Boolean {
    if (event.action == KeyEvent.ACTION_UP && suppressedKeys.remove(event.keyCode)) return true
    if (event.action != KeyEvent.ACTION_DOWN) return super.dispatchKeyEvent(event)
    if (event.repeatCount > 0 && suppressedKeys.contains(event.keyCode)) return true
    val config = leaderConfig
    if (config != null && matches(event, config["trigger"] as? Map<*, *>)) {
      leaderDeadline = SystemClock.uptimeMillis() + 2000L
      suppressedKeys.add(event.keyCode)
      return true
    }
    if (leaderDeadline > SystemClock.uptimeMillis() && !KeyEvent.isModifierKey(event.keyCode)) {
      leaderDeadline = 0L
      if (event.keyCode == KeyEvent.KEYCODE_ESCAPE) {
        suppressedKeys.add(event.keyCode)
        return true
      }
      val binding = (config?.get("bindings") as? List<*>)?.filterIsInstance<Map<*, *>>()
        ?.firstOrNull { matches(event, it) && enabledCommands.contains(it["command"]) }
      val command = binding?.get("command") as? String
      if (command != null) {
        suppressedKeys.add(event.keyCode)
        onCommand(mapOf("command" to command))
        return true
      }
    }
    val command = commandFor(event)?.takeIf { enabledCommands.contains(it) }
    if (command != null) {
      suppressedKeys.add(event.keyCode)
      onCommand(mapOf("command" to command))
      return true
    }
    suppressedKeys.remove(event.keyCode)
    return super.dispatchKeyEvent(event)
  }

  private fun matches(event: KeyEvent, shortcut: Map<*, *>?): Boolean {
    if (shortcut == null) return false
    val key = when (event.keyCode) {
      KeyEvent.KEYCODE_ESCAPE -> "escape"
      KeyEvent.KEYCODE_ENTER -> "enter"
      KeyEvent.KEYCODE_TAB -> "tab"
      KeyEvent.KEYCODE_DEL -> "backspace"
      KeyEvent.KEYCODE_DPAD_UP -> "arrowup"
      KeyEvent.KEYCODE_DPAD_DOWN -> "arrowdown"
      KeyEvent.KEYCODE_DPAD_LEFT -> "arrowleft"
      KeyEvent.KEYCODE_DPAD_RIGHT -> "arrowright"
      else -> event.getUnicodeChar(event.metaState and (KeyEvent.META_CTRL_MASK or KeyEvent.META_META_MASK).inv()).toChar().toString().lowercase()
    }
    val unshiftedKey = event.getUnicodeChar(event.metaState and (KeyEvent.META_CTRL_MASK or KeyEvent.META_META_MASK or KeyEvent.META_SHIFT_MASK).inv()).toChar().toString().lowercase()
    return (key == shortcut["key"] || event.isShiftPressed && unshiftedKey == shortcut["key"]) && event.isCtrlPressed == (shortcut["ctrlKey"] == true) &&
      event.isMetaPressed == (shortcut["metaKey"] == true) && event.isAltPressed == (shortcut["altKey"] == true) &&
      event.isShiftPressed == (shortcut["shiftKey"] == true)
  }

  private fun commandFor(event: KeyEvent): String? {
    if (event.repeatCount != 0 || !event.isCtrlPressed) return null
    return when {
      event.keyCode == KeyEvent.KEYCODE_C && event.isShiftPressed && !event.isAltPressed -> "copyThreadReference"
      event.keyCode == KeyEvent.KEYCODE_H && event.isShiftPressed && !event.isAltPressed -> "cycleHost"
      else -> null
    }
  }
}
