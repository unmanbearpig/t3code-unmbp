// First application module evaluated by index.ts. Native process/bootstrap time
// before JavaScript starts is not observable here and is excluded from cold timing.
export const mobileJsLaunch = { wall: Date.now(), monotonic: performance.now() };
