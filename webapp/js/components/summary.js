import { formatVolume } from "../utils/formatters.js";
import {
  getReturnField,
  getVolatilityField,
  getVolumeField,
} from "../utils/helpers.js";

export function updateSummaryCards(data, timeframe) {
  const returnField = getReturnField(timeframe);
  const volatilityField = getVolatilityField(timeframe);
  const volumeField = getVolumeField(timeframe);

  if (!data || data.length === 0) return;

  // Top Gainer
  const topGainer = [...data].sort(
    (a, b) => b[returnField] - a[returnField]
  )[0];
  document.getElementById("topGainer").textContent = topGainer.symbol;
  document.getElementById("topGainerChange").textContent = `+${topGainer[
    returnField
  ].toFixed(2)}%`;

  // Top Loser
  const topLoser = [...data].sort((a, b) => a[returnField] - b[returnField])[0];
  document.getElementById("topLoser").textContent = topLoser.symbol;
  document.getElementById("topLoserChange").textContent = `${topLoser[
    returnField
  ].toFixed(2)}%`;

  // Most Volatile
  const mostVolatile = [...data].sort(
    (a, b) => b[volatilityField] - a[volatilityField]
  )[0];
  document.getElementById("mostVolatile").textContent = mostVolatile.symbol;
  document.getElementById("mostVolatileValue").textContent = `${mostVolatile[
    volatilityField
  ].toFixed(2)}%`;

  // Highest Volume
  const highestVolume = [...data].sort(
    (a, b) => b[volumeField] - a[volumeField]
  )[0];
  document.getElementById("highestVolume").textContent = highestVolume.symbol;
  document.getElementById("highestVolumeValue").textContent = formatVolume(
    highestVolume[volumeField]
  );
}
