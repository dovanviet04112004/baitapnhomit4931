import { formatVolume } from "../utils/formatters.js";
import {
  getReturnField,
  getVolatilityField,
  getVolumeField,
  getOpenField,
  getCloseField,
  getHighField,
  getLowField,
} from "../utils/helpers.js";

export function renderTable(data, timeframe, filter) {
  const tbody = document.getElementById("tableBody");
  if (!tbody) return;

  const returnField = getReturnField(timeframe);
  const volatilityField = getVolatilityField(timeframe);
  const volumeField = getVolumeField(timeframe);
  const openField = getOpenField(timeframe);
  const closeField = getCloseField(timeframe);
  const highField = getHighField(timeframe);
  const lowField = getLowField(timeframe);

  // Filter data
  let filteredData = [...data];
  if (filter === "gainers") {
    filteredData = filteredData.filter((d) => d[returnField] > 0);
  } else if (filter === "losers") {
    filteredData = filteredData.filter((d) => d[returnField] < 0);
  }

  // Sort by return %
  filteredData.sort(
    (a, b) => Math.abs(b[returnField]) - Math.abs(a[returnField])
  );

  // Render rows
  tbody.innerHTML = filteredData
    .slice(0, 20)
    .map(
      (coin, index) => `
        <tr>
            <td>${index + 1}</td>
            <td>
                <div class="coin-info">
                    <span class="coin-name">${coin.name}</span>
                </div>
            </td>
            <td><span class="coin-symbol">${coin.symbol}</span></td>
            <td>$${coin[openField].toFixed(2)}</td>
            <td>$${coin[closeField].toFixed(2)}</td>
            <td>$${coin[highField].toFixed(2)}</td>
            <td>$${coin[lowField].toFixed(2)}</td>
            <td>
                <span class="price-change ${
                  coin[returnField] >= 0 ? "positive" : "negative"
                }">
                    ${coin[returnField] >= 0 ? "+" : ""}${coin[
        returnField
      ].toFixed(2)}%
                </span>
            </td>
            <td>${coin[volatilityField].toFixed(2)}%</td>
            <td>${formatVolume(coin[volumeField])}</td>
        </tr>
    `
    )
    .join("");
}
