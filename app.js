require("dotenv").config();

const http = require("http");
const path = require("path");
const express = require("express");
const { Server } = require("socket.io");
const Redis = require("ioredis");
const open = require("open").default;

const { config } = require("./src/config/env");
const { SYMBOLS, SIDES } = require("./src/config/constants");
const { log } = require("./src/config/logger");
const { RedisRepository } = require("./src/repositories/RedisRepository");
const { KafkaMirror } = require("./src/streams/kafkaMirror");
const { BinanceStream } = require("./src/streams/binanceStream");
const { PythonBinanceBridge } = require("./src/streams/pythonBinanceBridge");
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
    const pyBridge = new PythonBinanceBridge({
      pythonCmd: config.pythonCmd,
      symbols: SYMBOLS,
      onPrice: (tick) => marketService.onPriceTick(tick),
      maxRestarts: config.pyBinanceMaxRestarts,
      onFatal: () => {
        log("stream", "python-binance disabled; fallback price generator remains active");
      },
    });
    pyBridge.start();
  } else {
    const binance = new BinanceStream(SYMBOLS, (tick) => {
      marketService.onPriceTick(tick);
    });
    binance.start();
  }

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
}

bootstrap().catch((err) => {
  console.error("fatal bootstrap error:", err);
  process.exit(1);
});
