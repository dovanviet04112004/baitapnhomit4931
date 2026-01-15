// Configuration
const API_BASE_URL = "/api"; // Use relative path for same-origin requests
const USE_MOCK_DATA = false; // Set to false when backend is ready

// State
let currentTimeframe = "daily";
let currentFilter = "all";
let currentTheme = localStorage.getItem("theme") || "system";
let metricsData = null;
let priceChart = null;
let volumeChart = null;

// Initialize
document.addEventListener("DOMContentLoaded", () => {
  initializeTheme();
  initializeEventListeners();
  loadData();
});

// Theme Management
function initializeTheme() {
  // Set initial icon
  updateThemeIcon(currentTheme);
  applyTheme(currentTheme);

  // Listen for system changes to update icon if in system mode (optional, but good UX)
  window
    .matchMedia("(prefers-color-scheme: dark)")
    .addEventListener("change", (e) => {
      if (currentTheme === "system") {
        applyTheme("system");
        updateThemeIcon("system");
      }
    });
}

function applyTheme(theme) {
  let resolvedTheme = theme;

  if (theme === "system") {
    resolvedTheme = window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  document.documentElement.setAttribute("data-theme", resolvedTheme);

  // Update charts if they exist
  if (metricsData) {
    renderCharts(metricsData);
  }
}

function toggleTheme() {
  // Simple toggle logic: If currently dark (or system resolving to dark), go light. Else go dark.
  // For manual toggle, we usually step out of 'system' mode.

  const isDark = document.documentElement.getAttribute("data-theme") === "dark";
  const newTheme = isDark ? "light" : "dark";

  currentTheme = newTheme;
  localStorage.setItem("theme", newTheme);
  applyTheme(newTheme);
  updateThemeIcon(newTheme);
}

function updateThemeIcon(theme) {
  const iconSpan = document.getElementById("themeIcon");
  let displayIcon = "☀️"; // Default to sun (for light mode)

  if (theme === "dark") {
    displayIcon = "🌙";
  } else if (theme === "system") {
    // Resolve system
    const isSystemDark = window.matchMedia(
      "(prefers-color-scheme: dark)"
    ).matches;
    displayIcon = isSystemDark ? "🌙" : "☀️";
  }

  iconSpan.textContent = displayIcon;
}

// Event Listeners
function initializeEventListeners() {
  // Theme toggle
  const themeBtn = document.getElementById("themeToggle");
  if (themeBtn) {
    themeBtn.addEventListener("click", toggleTheme);
  }

  // Tab buttons
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const timeframe = e.currentTarget.dataset.timeframe;
      switchTimeframe(timeframe);
    });
  });

  // Filter buttons
  document.querySelectorAll(".filter-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      const filter = e.currentTarget.dataset.filter;
      switchFilter(filter);
    });
  });
}

// Switch Timeframe
function switchTimeframe(timeframe) {
  currentTimeframe = timeframe;

  // Update active tab
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.classList.remove("active");
    if (btn.dataset.timeframe === timeframe) {
      btn.classList.add("active");
    }
  });

  // Reload data
  loadData();
}

// Switch Filter
function switchFilter(filter) {
  currentFilter = filter;

  // Update active filter button
  document.querySelectorAll(".filter-btn").forEach((btn) => {
    btn.classList.remove("active");
    if (btn.dataset.filter === filter) {
      btn.classList.add("active");
    }
  });

  // Re-render table
  renderTable(metricsData);
}

// Load Data
async function loadData() {
  showLoading(true);

  try {
    if (USE_MOCK_DATA) {
      metricsData = generateMockData(currentTimeframe);
    } else {
      const response = await fetch(
        `${API_BASE_URL}/${currentTimeframe}-metrics`
      );
      metricsData = await response.json();
    }

    renderDashboard(metricsData);
  } catch (error) {
    console.error("Error loading data:", error);
    alert("Failed to load data. Using mock data instead.");
    metricsData = generateMockData(currentTimeframe);
    renderDashboard(metricsData);
  } finally {
    showLoading(false);
  }
}

// Render Dashboard
function renderDashboard(data) {
  updateHeaderStats(data);
  updateSummaryCards(data);
  renderCharts(data);
  renderTable(data);
}

// Update Header Stats
function updateHeaderStats(data) {
  document.getElementById("totalCoins").textContent = data.length;
  document.getElementById("lastUpdated").textContent =
    new Date().toLocaleString();
}

// Update Summary Cards
function updateSummaryCards(data) {
  const returnField = getReturnField();
  const volatilityField = getVolatilityField();
  const volumeField = getVolumeField();

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

// Helper to get chart colors based on theme
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
    tooltipBorder: isDark
      ? "rgba(148, 163, 184, 0.2)"
      : "rgba(148, 163, 184, 0.2)",
  };
}

// Render Charts
function renderCharts(data) {
  const topCoins = [...data]
    .sort((a, b) => b[getVolumeField()] - a[getVolumeField()])
    .slice(0, 10);

  renderPriceChart(topCoins);
  renderVolumeChart(topCoins);
}

// Render Price Chart
function renderPriceChart(data) {
  const ctx = document.getElementById("priceChart").getContext("2d");
  const returnField = getReturnField();
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

// Render Volume Chart
function renderVolumeChart(data) {
  const ctx = document.getElementById("volumeChart").getContext("2d");
  const volumeField = getVolumeField();
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

// Render Table
function renderTable(data) {
  const tbody = document.getElementById("tableBody");
  const returnField = getReturnField();
  const volatilityField = getVolatilityField();
  const volumeField = getVolumeField();
  const openField = getOpenField();
  const closeField = getCloseField();
  const highField = getHighField();
  const lowField = getLowField();

  // Filter data
  let filteredData = [...data];
  if (currentFilter === "gainers") {
    filteredData = filteredData.filter((d) => d[returnField] > 0);
  } else if (currentFilter === "losers") {
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

// Helper Functions
function getReturnField() {
  const fields = {
    daily: "return_pct_day",
    weekly: "return_pct_week",
    monthly: "return_pct_month",
  };
  return fields[currentTimeframe];
}

function getVolatilityField() {
  const fields = {
    daily: "volatility_day",
    weekly: "volatility_week",
    monthly: "volatility_month",
  };
  return fields[currentTimeframe];
}

function getVolumeField() {
  const fields = {
    daily: "volume_sum_day",
    weekly: "volume_sum_week",
    monthly: "volume_sum_month",
  };
  return fields[currentTimeframe];
}

function getOpenField() {
  const fields = {
    daily: "open_price",
    weekly: "open_price_week",
    monthly: "open_price_month",
  };
  return fields[currentTimeframe];
}

function getCloseField() {
  const fields = {
    daily: "close_price",
    weekly: "close_price_week",
    monthly: "close_price_month",
  };
  return fields[currentTimeframe];
}

function getHighField() {
  const fields = {
    daily: "high_price",
    weekly: "high_price_week",
    monthly: "high_price_month",
  };
  return fields[currentTimeframe];
}

function getLowField() {
  const fields = {
    daily: "low_price",
    weekly: "low_price_week",
    monthly: "low_price_month",
  };
  return fields[currentTimeframe];
}

function formatVolume(value) {
  if (value >= 1e9) return (value / 1e9).toFixed(2) + "B";
  if (value >= 1e6) return (value / 1e6).toFixed(2) + "M";
  if (value >= 1e3) return (value / 1e3).toFixed(2) + "K";
  return value.toFixed(2);
}

function showLoading(show) {
  const overlay = document.getElementById("loadingOverlay");
  if (show) {
    overlay.classList.add("active");
  } else {
    overlay.classList.remove("active");
  }
}

// Mock Data Generator
function generateMockData(timeframe) {
  const coins = [
    { id: "bitcoin", symbol: "BTC", name: "Bitcoin" },
    { id: "ethereum", symbol: "ETH", name: "Ethereum" },
    { id: "binancecoin", symbol: "BNB", name: "Binance Coin" },
    { id: "cardano", symbol: "ADA", name: "Cardano" },
    { id: "solana", symbol: "SOL", name: "Solana" },
    { id: "ripple", symbol: "XRP", name: "Ripple" },
    { id: "polkadot", symbol: "DOT", name: "Polkadot" },
    { id: "dogecoin", symbol: "DOGE", name: "Dogecoin" },
    { id: "avalanche", symbol: "AVAX", name: "Avalanche" },
    { id: "polygon", symbol: "MATIC", name: "Polygon" },
    { id: "chainlink", symbol: "LINK", name: "Chainlink" },
    { id: "litecoin", symbol: "LTC", name: "Litecoin" },
    { id: "uniswap", symbol: "UNI", name: "Uniswap" },
    { id: "stellar", symbol: "XLM", name: "Stellar" },
    { id: "cosmos", symbol: "ATOM", name: "Cosmos" },
  ];

  return coins.map((coin) => {
    const basePrice = Math.random() * 1000 + 10;
    const returnPct = (Math.random() - 0.5) * 20; // -10% to +10%
    const volatility = Math.random() * 15 + 2; // 2% to 17%

    const openPrice = basePrice;
    const closePrice = openPrice * (1 + returnPct / 100);
    const highPrice =
      Math.max(openPrice, closePrice) * (1 + Math.random() * 0.05);
    const lowPrice =
      Math.min(openPrice, closePrice) * (1 - Math.random() * 0.05);
    const volume = Math.random() * 1e9 + 1e8;

    const data = {
      coin_id: coin.id,
      symbol: coin.symbol,
      name: coin.name,
    };

    if (timeframe === "daily") {
      data.open_price = openPrice;
      data.close_price = closePrice;
      data.high_price = highPrice;
      data.low_price = lowPrice;
      data.return_pct_day = returnPct;
      data.volatility_day = volatility;
      data.volume_sum_day = volume;
    } else if (timeframe === "weekly") {
      data.open_price_week = openPrice;
      data.close_price_week = closePrice;
      data.high_price_week = highPrice;
      data.low_price_week = lowPrice;
      data.return_pct_week = returnPct * 1.5;
      data.volatility_week = volatility * 1.2;
      data.volume_sum_week = volume * 7;
    } else if (timeframe === "monthly") {
      data.open_price_month = openPrice;
      data.close_price_month = closePrice;
      data.high_price_month = highPrice;
      data.low_price_month = lowPrice;
      data.return_pct_month = returnPct * 3;
      data.volatility_month = volatility * 1.5;
      data.volume_sum_month = volume * 30;
    }

    return data;
  });
}
