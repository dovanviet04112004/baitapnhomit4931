import { formatVolume } from "../utils/formatters.js";
import { getReturnField, getVolumeField } from "../utils/helpers.js";

let priceChart = null;
let volumeChart = null;

export function renderCharts(data, timeframe) {
  const topCoins = [...data]
    .sort((a, b) => b[getVolumeField(timeframe)] - a[getVolumeField(timeframe)])
    .slice(0, 10);

  renderPriceChart(topCoins, timeframe);
  renderVolumeChart(topCoins, timeframe);
}

function getChartColors() {
  const isDark =
    document.documentElement.getAttribute("data-theme") === "dark" ||
    (document.documentElement.getAttribute("data-theme") === "light"
      ? false
      : window.matchMedia("(prefers-color-scheme: dark)").matches);

  return {
    text: isDark ? "#94a3b8" : "#64748b",
    grid: isDark ? "rgba(148, 163, 184, 0.1)" : "rgba(148, 163, 184, 0.2)",
    tooltipBg: isDark ? "rgba(11, 14, 20, 0.95)" : "rgba(255, 255, 255, 0.95)",
    tooltipText: isDark ? "#f1f5f9" : "#0f172a",
    tooltipBorder: "rgba(148, 163, 184, 0.2)",
  };
}

function renderPriceChart(data, timeframe) {
  const canvas = document.getElementById("priceChart");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  const returnField = getReturnField(timeframe);
  const colors = getChartColors();

  if (priceChart) {
    priceChart.destroy();
  }

  priceChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: data.map((d) => d.symbol),
      datasets: [
        {
          label: "Return %",
          data: data.map((d) => d[returnField]),
          backgroundColor: data.map((d) =>
            d[returnField] >= 0
              ? "rgba(0, 255, 157, 0.6)"
              : "rgba(255, 59, 59, 0.6)"
          ),
          borderColor: data.map((d) =>
            d[returnField] >= 0 ? "#00ff9d" : "#ff3b3b"
          ),
          borderWidth: 1,
          borderRadius: 4,
          hoverBackgroundColor: data.map((d) =>
            d[returnField] >= 0
              ? "rgba(0, 255, 157, 0.8)"
              : "rgba(255, 59, 59, 0.8)"
          ),
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: colors.tooltipBg,
          titleColor: colors.tooltipText,
          bodyColor: colors.tooltipText,
          borderColor: colors.tooltipBorder,
          borderWidth: 1,
          padding: 12,
          displayColors: false,
          callbacks: {
            label: (context) => `Return: ${context.parsed.y.toFixed(2)}%`,
          },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: {
            color: colors.grid,
          },
          ticks: {
            color: colors.text,
            callback: (value) => value + "%",
          },
        },
        x: {
          grid: {
            display: false,
          },
          ticks: {
            color: colors.text,
            font: {
              family: "Outfit",
            },
          },
        },
      },
    },
  });
}

function renderVolumeChart(data, timeframe) {
  const canvas = document.getElementById("volumeChart");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  const volumeField = getVolumeField(timeframe);
  const colors = getChartColors();

  if (volumeChart) {
    volumeChart.destroy();
  }

  volumeChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: data.map((d) => d.symbol),
      datasets: [
        {
          label: "Volume",
          data: data.map((d) => d[volumeField]),
          backgroundColor: "rgba(59, 130, 246, 0.6)", // Solid blue/purple
          borderColor: "#3b82f6",
          borderWidth: 1,
          borderRadius: 4,
          hoverBackgroundColor: "rgba(59, 130, 246, 0.8)",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: colors.tooltipBg,
          titleColor: colors.tooltipText,
          bodyColor: colors.tooltipText,
          borderColor: colors.tooltipBorder,
          borderWidth: 1,
          padding: 12,
          displayColors: false,
          callbacks: {
            label: (context) => `Volume: ${formatVolume(context.parsed.y)}`,
          },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: {
            color: colors.grid,
          },
          ticks: {
            color: colors.text,
            callback: (value) => formatVolume(value),
          },
        },
        x: {
          grid: {
            display: false,
          },
          ticks: {
            color: colors.text,
            font: {
              family: "Outfit",
            },
          },
        },
      },
    },
  });
}
