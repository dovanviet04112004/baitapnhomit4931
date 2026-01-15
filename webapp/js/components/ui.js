export function showLoading(show) {
  const overlay = document.getElementById("loadingOverlay");
  if (!overlay) return;

  if (show) {
    overlay.classList.add("active");
  } else {
    overlay.classList.remove("active");
  }
}
