export function showDialog(title, message) {
  const dialog = document.getElementById("infoDialog");
  if (!dialog) return;

  const titleEl = document.getElementById("dialogTitle");
  const msgEl = document.getElementById("dialogMessage");

  if (titleEl) titleEl.textContent = title;
  if (msgEl) msgEl.textContent = message;

  dialog.showModal();
}

export function closeDialog() {
  const dialog = document.getElementById("infoDialog");
  if (dialog) {
    dialog.close();
  }
}

document.addEventListener("DOMContentLoaded", () => {
  const closeBtn = document.getElementById("dialogCloseBtn");
  if (closeBtn) {
    closeBtn.addEventListener("click", closeDialog);
  }
});
