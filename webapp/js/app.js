import { fetchMetrics } from "./services/api.js";
import { initializeTheme } from "./components/theme.js";
import { showLoading } from "./components/ui.js";
import { updateHeaderStats } from "./components/header.js";
import { updateSummaryCards } from "./components/summary.js";
import { renderCharts } from "./components/charts.js";
import { renderTable } from "./components/table.js";
import { showDialog } from "./components/dialog.js";
import { generateMockData } from "./services/mockData.js"; // Import mock generator as fallback

// State
let currentTimeframe = "daily";
let currentFilter = "all";
let metricsData = null;

// Initialize
document.addEventListener("DOMContentLoaded", () => {
  initializeTheme();
  initializeEventListeners();
  loadData();
});

// Event Listeners
function initializeEventListeners() {
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

  // Listen for theme changes to update charts (if needed, chart.js usually needs re-render)
  window.addEventListener("themeChanged", () => {
    if (metricsData) {
      renderCharts(metricsData, currentTimeframe);
    }
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
  renderTable(metricsData, currentTimeframe, currentFilter);
}

// Load Data
async function loadData() {
  showLoading(true);

  try {
    const data = await fetchMetrics(currentTimeframe);
    metricsData = data;
    renderDashboard(metricsData);
  } catch (error) {
    console.warn("API failed, switching to mock data");

    // Show Dialog
    showDialog(
      "Connection Failed",
      "Could not connect to the live API. Showing demonstration data instead."
    );

    // Fallback to mock data
    metricsData = generateMockData(currentTimeframe);
    renderDashboard(metricsData);
  } finally {
    showLoading(false);
  }
}

// Render Dashboard
function renderDashboard(data) {
  updateHeaderStats(data);
  updateSummaryCards(data, currentTimeframe);
  renderCharts(data, currentTimeframe);
  renderTable(data, currentTimeframe, currentFilter);
}
