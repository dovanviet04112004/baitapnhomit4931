export function initializeTheme() {
  const storedTheme = localStorage.getItem("theme") || "system";
  applyTheme(storedTheme);
  updateThemeIcon(storedTheme);

  // System theme listener
  window
    .matchMedia("(prefers-color-scheme: dark)")
    .addEventListener("change", (e) => {
      const currentTheme = localStorage.getItem("theme") || "system";
      if (currentTheme === "system") {
        applyTheme("system");
        updateThemeIcon("system");
      }
    });

  // Toggle button listener
  const themeBtn = document.getElementById("themeToggle");
  if (themeBtn) {
    themeBtn.addEventListener("click", toggleTheme);
  }
}

export function getCurrentTheme() {
  return localStorage.getItem("theme") || "system";
}

function applyTheme(theme) {
  let resolvedTheme = theme;

  if (theme === "system") {
    resolvedTheme = window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  document.documentElement.setAttribute("data-theme", resolvedTheme);

  // Dispatch custom event for charts to update
  window.dispatchEvent(
    new CustomEvent("themeChanged", { detail: { theme: resolvedTheme } })
  );
}

function updateThemeIcon(theme) {
  const iconSpan = document.getElementById("themeIcon");
  if (!iconSpan) return;

  let displayIcon = "☀️";

  if (theme === "dark") {
    displayIcon = "🌙";
  } else if (theme === "system") {
    const isSystemDark = window.matchMedia(
      "(prefers-color-scheme: dark)"
    ).matches;
    displayIcon = isSystemDark ? "🌙" : "☀️";
  }

  iconSpan.textContent = displayIcon;
}

function toggleTheme() {
  const currentTheme = localStorage.getItem("theme") || "system";
  const isDark = document.documentElement.getAttribute("data-theme") === "dark";

  // Logic: if system/light -> go dark. If dark -> go light.
  // Or cleaner: simple toggle based on resolved state
  const newTheme = isDark ? "light" : "dark";

  localStorage.setItem("theme", newTheme);
  applyTheme(newTheme);
  updateThemeIcon(newTheme);
}
