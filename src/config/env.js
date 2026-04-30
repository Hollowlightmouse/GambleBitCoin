const path = require("path");

function toNumber(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toBoolean(value, fallback) {
  if (value === undefined) return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

const config = {
  port: toNumber(process.env.PORT, 3000),
  redisUrl: process.env.REDIS_URL || "redis://localhost:6379",
  kafkaBrokers: (process.env.KAFKA_BROKERS || "localhost:9092")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  kafkaPriceTopic: process.env.KAFKA_PRICE_TOPIC || "market.prices.raw",
  kafkaBetTopic: process.env.KAFKA_BET_TOPIC || "market.bets.events",
  kafkaRoundTopic: process.env.KAFKA_ROUND_TOPIC || "market.round.events",
  kafkaAlertsTopic: process.env.KAFKA_ALERTS_TOPIC || "market.alerts",
  roundSeconds: toNumber(process.env.ROUND_SECONDS, 20),
  lockSeconds: toNumber(process.env.LOCK_SECONDS, 5),
  initialBalance: toNumber(process.env.INITIAL_BALANCE, 2000),
  betMin: toNumber(process.env.BET_MIN, 10),
  betMax: toNumber(process.env.BET_MAX, 300),
  chatLimit: toNumber(process.env.CHAT_LIMIT, 200),
  alertLimit: toNumber(process.env.ALERT_LIMIT, 50),
  roundHistoryLimit: toNumber(process.env.ROUND_HISTORY_LIMIT, 100),
  enablePriceFallback: toBoolean(process.env.ENABLE_PRICE_FALLBACK, true),
  priceStaleMs: toNumber(process.env.PRICE_STALE_MS, 12000),
  usePyBinance: toBoolean(process.env.USE_PY_BINANCE, true),
  pythonCmd: process.env.PYTHON_CMD || "python",
  pyBinanceMaxRestarts: toNumber(process.env.PY_BINANCE_MAX_RESTARTS, 6),
  spikeUpPct: toNumber(process.env.PRICE_SPIKE_UP_PCT, 2.0),
  spikeDownPct: toNumber(process.env.PRICE_SPIKE_DOWN_PCT, 2.0),
  spikeWindowSec: toNumber(process.env.PRICE_SPIKE_WINDOW_SEC, 30),
  autoOpenBrowser: toBoolean(process.env.AUTO_OPEN_BROWSER, true),
  publicDir: path.join(process.cwd(), "src", "public"),
};

module.exports = { config };
