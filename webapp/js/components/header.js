export function updateHeaderStats(data) {
  const totalCoinsEl = document.getElementById("totalCoins");

  if (totalCoinsEl) totalCoinsEl.textContent = data.length;
}
