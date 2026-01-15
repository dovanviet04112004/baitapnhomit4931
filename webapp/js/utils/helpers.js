export function getReturnField(timeframe) {
  const fields = {
    daily: "return_pct_day",
    weekly: "return_pct_week",
    monthly: "return_pct_month",
  };
  return fields[timeframe];
}

export function getVolatilityField(timeframe) {
  const fields = {
    daily: "volatility_day",
    weekly: "volatility_week",
    monthly: "volatility_month",
  };
  return fields[timeframe];
}

export function getVolumeField(timeframe) {
  const fields = {
    daily: "volume_sum_day",
    weekly: "volume_sum_week",
    monthly: "volume_sum_month",
  };
  return fields[timeframe];
}

export function getOpenField(timeframe) {
  const fields = {
    daily: "open_price",
    weekly: "open_price_week",
    monthly: "open_price_month",
  };
  return fields[timeframe];
}

export function getCloseField(timeframe) {
  const fields = {
    daily: "close_price",
    weekly: "close_price_week",
    monthly: "close_price_month",
  };
  return fields[timeframe];
}

export function getHighField(timeframe) {
  const fields = {
    daily: "high_price",
    weekly: "high_price_week",
    monthly: "high_price_month",
  };
  return fields[timeframe];
}

export function getLowField(timeframe) {
  const fields = {
    daily: "low_price",
    weekly: "low_price_week",
    monthly: "low_price_month",
  };
  return fields[timeframe];
}
