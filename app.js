require("dotenv").config();

const http = require("http");
const path = require("path");
const express = require("express");
const { Server } = require("socket.io");
const Redis = require("ioredis");
const open = require("open").default;

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
const { SYMBOLS, SIDES } = require("./src/config/constants");
const { log } = require("./src/config/logger");
const { RedisRepository } = require("./src/repositories/RedisRepository");
const { KafkaMirror } = require("./src/streams/kafkaMirror");
const { BinanceKafkaProducer } = require("./src/streams/binanceKafkaProducer");
const { KafkaStreamConsumer } = require("./src/streams/kafkaStreamConsumer");
const { UserService } = require("./src/services/userService");
const { LeaderboardService } = require("./src/services/leaderboardService");
const { ChatService } = require("./src/services/chatService");
const { MarketService } = require("./src/services/marketService");
const { BetService } = require("./src/services/betService");
const { registerHealthController } = require("./src/controllers/healthController");
const { registerUserController } = require("./src/controllers/userController");
const { registerAdminController } = require("./src/controllers/adminController");
const { registerSocketHandlers } = require("./src/sockets/registerSocketHandlers");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static(config.publicDir));

app.get("/", (_req, res) => {
  res.sendFile(path.join(config.publicDir, "index.html"));
});

const redis = new Redis(config.redisUrl);
const repo = new RedisRepository(redis, config);
const kafkaMirror = new KafkaMirror(config.kafkaBrokers);

const userService = new UserService(repo, config);
const leaderboardService = new LeaderboardService(repo);
const chatService = new ChatService(repo);

const marketService = new MarketService({
  symbols: SYMBOLS,
  repo,
  io,
  config,
  leaderboardService,
  kafkaMirror,
});

const betService = new BetService({
  repo,
  config,
  symbols: SYMBOLS,
  sides: SIDES,
  marketService,
  leaderboardService,
  kafkaMirror,
});

registerHealthController(app, { redis, kafkaMirror, symbols: SYMBOLS, config });
registerUserController(app, { userService, marketService, chatService, leaderboardService });
registerAdminController(app, { repo, marketService, config });

registerSocketHandlers(io, {
  userService,
  marketService,
  betService,
  chatService,
  leaderboardService,
  repo,
});

async function bootstrap() {
  process.on("unhandledRejection", (reason) => {
    console.error("unhandledRejection:", reason);
  });

  process.on("uncaughtException", (err) => {
    console.error("uncaughtException:", err);
  });

  await marketService.hydrateTickSizes();
  await kafkaMirror.connect();

  if (config.usePyBinance) {
    const binanceProducer = new BinanceKafkaProducer({
      pythonCmd: config.pythonCmd,
      symbols: SYMBOLS,
      kafkaMirror,
      topic: config.kafkaPriceTopic,
      maxRestarts: config.pyBinanceMaxRestarts,
      onFatal: () => {
        log("stream", "python-binance disabled; fallback price generator remains active");
      },
    });
    binanceProducer.start();
  } else {
    log("stream", "python-binance disabled; price producer not started");
  }

  const streamConsumer = new KafkaStreamConsumer({
    kafkaMirror,
    topics: [config.kafkaPriceTopic, config.kafkaAlertsTopic],
    groupId: "market-consumer",
    onMessage: (topic, payload) => {
      if (topic === config.kafkaPriceTopic) {
        marketService.onPriceTick({
          symbol: String(payload.symbol).toUpperCase(),
          price: Number(payload.price),
          ts: Number(payload.ts || Date.now()),
          source: payload.source || "kafka",
        });
        return;
      }

      if (topic === config.kafkaAlertsTopic) {
        io.emit("anomaly_alert", payload);
      }
    },
  });
  await streamConsumer.start();

  marketService.startRoundLoop();
  marketService.startFallbackPriceGenerator();

  server.listen(config.port, () => {
    const appUrl = `http://localhost:${config.port}`;
    log("app", `running on ${appUrl}`);
    log("app", `open in browser: ${appUrl}`);
    log("rules", `round=${config.roundSeconds}s lock=${config.lockSeconds}s initial=${config.initialBalance} min=${config.betMin} max=${config.betMax}`);
    log("services", `redis=${config.redisUrl} kafka=${config.kafkaBrokers.join(",") || "disabled"}`);
    log("stream", config.usePyBinance ? `python-binance via ${config.pythonCmd}` : "native ws");
    log("markets", SYMBOLS.join(", "));

    if (config.autoOpenBrowser) {
      open(appUrl).catch((err) => {
        console.error("browser open error:", err.message);
      });
    }
  });

  process.on("SIGINT", async () => {
    await streamConsumer.stop();
    process.exit(0);
  });
}

bootstrap().catch((err) => {
  console.error("fatal bootstrap error:", err);
  process.exit(1);
});
