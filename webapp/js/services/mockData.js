export function generateMockData(timeframe) {
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
