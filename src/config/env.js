function toNumber(value, fallback) {
  var num = Number(value);
  return isFinite(num) ? num : fallback;
}

function toBoolean(value, fallback) {
  if (value === undefined) return fallback;
  var str = String(value).toLowerCase();
  return str === "1" || str === "true" || str === "yes" || str === "on";
}

var config = {
  port: toNumber(process.env.PORT, 3000),
  roundSeconds: toNumber(process.env.ROUND_SECONDS, 20),
  lockSeconds: toNumber(process.env.LOCK_SECONDS, 5),
  initialBalance: toNumber(process.env.INITIAL_BALANCE, 2000),
  betMin: toNumber(process.env.BET_MIN, 10),
  betMax: toNumber(process.env.BET_MAX, 300),
  chatLimit: toNumber(process.env.CHAT_LIMIT, 200),
  roundHistoryLimit: toNumber(process.env.ROUND_HISTORY_LIMIT, 100),
  enablePriceFallback: toBoolean(process.env.ENABLE_PRICE_FALLBACK, true),
  priceStaleMs: toNumber(process.env.PRICE_STALE_MS, 12000),
  usePyBinance: toBoolean(process.env.USE_PY_BINANCE, false),
  pythonCmd: process.env.PYTHON_CMD || "python",
  pyBinanceMaxRestarts: toNumber(process.env.PY_BINANCE_MAX_RESTARTS, 6),
  autoOpenBrowser: toBoolean(process.env.AUTO_OPEN_BROWSER, false),
  binanceTld: (process.env.BINANCE_TLD || "com").trim(),
  binanceWsTimeout: toNumber(process.env.BINANCE_WS_TIMEOUT, 30),
  publicDir: require("path").join(process.cwd(), "src", "public"),
  kafkaBrokers: (process.env.KAFKA_BROKERS || "").split(",").map(function(s) { return s.trim(); }).filter(Boolean),
};

module.exports = { config: config };
