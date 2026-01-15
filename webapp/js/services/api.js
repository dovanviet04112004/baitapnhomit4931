import { API_BASE_URL, USE_MOCK_DATA } from "../config.js";
import { generateMockData } from "./mockData.js";

export async function fetchMetrics(timeframe) {
  if (USE_MOCK_DATA) {
    return generateMockData(timeframe);
  }

  try {
    const response = await fetch(`${API_BASE_URL}/${timeframe}-metrics`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.warn("API fetch failed, falling back to mock data", error);
    throw error; // Re-throw to handle in UI
  }
}
