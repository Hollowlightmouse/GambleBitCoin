const https = require("https");
const { v4: uuidv4 } = require("uuid");
const { Round } = require("../models/Round");
const { determineResult } = require("./priceUtils");
const { DEFAULT_TICK_SIZE } = require("../config/constants");

class MarketService {
  constructor({ symbols, repo, io, config, leaderboardService, kafkaMirror }) {
    this.symbols = symbols;
    this.repo = repo;
    this.io = io;
    this.config = config;
    this.leaderboardService = leaderboardService;
    this.kafkaMirror = kafkaMirror;

    this.state = {};
    for (const symbol of symbols) {
      this.state[symbol] = {
        latestPrice: null,
        latestPriceTs: 0,
        tickSize: DEFAULT_TICK_SIZE[symbol],
        currentRound: null,
      };
    }
  }

  room(symbol) {
    return `room:${symbol}`;
  }

  getState(symbol) {
    return this.state[symbol];
  }

  getCurrentRound(symbol) {
    return this.state[symbol]?.currentRound || null;
  }

  async fetchTickSizes() {
    const symbols = this.symbols;
    return new Promise((resolve) => {
      const req = https.get("https://api.binance.com/api/v3/exchangeInfo", (res) => {
        let data = "";
        res.on("data", (chunk) => {
          data += chunk;
        });
        res.on("end", () => {
          try {
            const parsed = JSON.parse(data);
            const output = { ...DEFAULT_TICK_SIZE };
            for (const symbolInfo of parsed.symbols || []) {
              if (!symbols.includes(symbolInfo.symbol)) continue;
              const filter = (symbolInfo.filters || []).find((f) => f.filterType === "PRICE_FILTER");
              if (filter && Number(filter.tickSize) > 0) {
                output[symbolInfo.symbol] = Number(filter.tickSize);
              }
            }
            resolve(output);
          } catch (_err) {
            resolve({ ...DEFAULT_TICK_SIZE });
          }
        });
      });

      req.on("error", () => resolve({ ...DEFAULT_TICK_SIZE }));
      req.setTimeout(5000, () => {
        req.destroy();
        resolve({ ...DEFAULT_TICK_SIZE });
      });
    });
  }

  async hydrateTickSizes() {
    const tickSizes = await this.fetchTickSizes();
    for (const symbol of this.symbols) {
      this.state[symbol].tickSize = tickSizes[symbol] || DEFAULT_TICK_SIZE[symbol];
    }
  }

  async onPriceTick({ symbol, price, ts, source = "binance_trade" }) {
    if (!this.state[symbol] || !price || Number.isNaN(price)) return;
    this.state[symbol].latestPrice = Number(price);
    this.state[symbol].latestPriceTs = Date.now();

    this.io.to(this.room(symbol)).emit("price_tick", { symbol, price, ts });

    await this.kafkaMirror.send("market.prices.raw", {
      symbol,
      price,
      ts,
      source,
    });
  }

  async openRound(symbol) {
    const market = this.state[symbol];
    if (!market.latestPrice) return;

    const now = Date.now();
    const round = new Round({
      id: uuidv4(),
      symbol,
      startPrice: market.latestPrice,
      startAt: now,
      endAt: now + this.config.roundSeconds * 1000,
      lockAt: now + (this.config.roundSeconds - this.config.lockSeconds) * 1000,
    });

    market.currentRound = round;
    await this.repo.saveRound(symbol, round);

    this.io.to(this.room(symbol)).emit("round_started", {
      symbol,
      round,
      secondsLeft: this.config.roundSeconds,
    });

    await this.kafkaMirror.send("market.round.events", {
      type: "round_started",
      ts: now,
      symbol,
      roundId: round.id,
      startPrice: round.startPrice,
      endAt: round.endAt,
      lockAt: round.lockAt,
    });
  }

  async settleRound(symbol) {
    const market = this.state[symbol];
    const round = market.currentRound;
    if (!round || round.status === "closed") return;

    round.status = "closed";
    round.endPrice = Number(market.latestPrice || round.startPrice);
    round.result = determineResult(round.startPrice, round.endPrice, market.tickSize);

    const bets = await this.repo.getRoundBets(symbol, round.id);
    const winners = bets
      .filter((bet) => bet.side === round.result)
      .sort((a, b) => a.timestamp - b.timestamp);

    const bonusMap = {};
    if (winners[0]) bonusMap[winners[0].userId] = 1.08;
    if (winners[1]) bonusMap[winners[1].userId] = 1.06;

    const payouts = [];
    for (const bet of bets) {
      const user = await this.repo.getUser(bet.userId);
      if (!user) continue;

      let payout = 0;
      let won = false;
      let bonusMultiplier = 1;

      if (bet.side === round.result) {
        won = true;
        payout = bet.amount * 2;
        if (bonusMap[user.id]) {
          bonusMultiplier = bonusMap[user.id];
          payout = payout * bonusMultiplier;
        }
        payout = Number(payout.toFixed(6));
        user.credit(payout);
      }

      await this.repo.setUserBalanceAndStatus(user.id, user.balance, user.blocked);
      await this.leaderboardService.updateForUser(symbol, user);

      payouts.push({
        userId: user.id,
        userName: user.name,
        betSide: bet.side,
        amount: bet.amount,
        won,
        payout,
        balance: user.balance,
        bonusMultiplier,
      });
    }

    await this.repo.saveRound(symbol, round);
    await this.repo.appendRoundHistory(symbol, {
      id: round.id,
      symbol,
      startPrice: round.startPrice,
      endPrice: round.endPrice,
      result: round.result,
      closedAt: Date.now(),
      bets: bets.length,
    });

    this.io.to(this.room(symbol)).emit("round_ended", {
      symbol,
      round: {
        id: round.id,
        startPrice: round.startPrice,
        endPrice: round.endPrice,
        result: round.result,
      },
      payouts,
    });

    const boards = await this.leaderboardService.getBoards(symbol);
    this.io.to(this.room(symbol)).emit("leaderboard_updated", {
      symbol,
      roomBoard: boards.roomBoard,
      globalBoard: boards.globalBoard,
    });

    await this.kafkaMirror.send("market.round.events", {
      type: "round_ended",
      ts: Date.now(),
      symbol,
      roundId: round.id,
      startPrice: round.startPrice,
      endPrice: round.endPrice,
      result: round.result,
      bets: bets.length,
      winners: winners.length,
    });

    setTimeout(() => this.openRound(symbol), 800);
  }

  startRoundLoop() {
    setInterval(async () => {
      const now = Date.now();

      for (const symbol of this.symbols) {
        const market = this.state[symbol];
        const round = market.currentRound;

        if (!round) {
          await this.openRound(symbol);
          continue;
        }

        const msLeft = Math.max(0, round.endAt - now);
        const secondsLeft = Math.ceil(msLeft / 1000);

        if (!round.locked && now >= round.lockAt) {
          round.locked = true;
          await this.repo.saveRound(symbol, round);
          this.io.to(this.room(symbol)).emit("round_locked", {
            symbol,
            roundId: round.id,
            secondsLeft,
          });
          await this.kafkaMirror.send("market.round.events", {
            type: "round_locked",
            ts: now,
            symbol,
            roundId: round.id,
          });
        }

        this.io.to(this.room(symbol)).emit("round_timer", {
          symbol,
          roundId: round.id,
          secondsLeft,
          lock: round.locked,
        });

        if (now >= round.endAt) {
          await this.settleRound(symbol);
        }
      }
    }, 1000);
  }

  startFallbackPriceGenerator() {
    if (!this.config.enablePriceFallback) return;

    setInterval(() => {
      const now = Date.now();

      for (const symbol of this.symbols) {
        const market = this.state[symbol];
        const isStale = now - market.latestPriceTs > this.config.priceStaleMs;
        if (!isStale) continue;

        const seed =
          market.latestPrice ||
          (symbol === "ETHUSDT" ? 3200 : symbol === "SOLUSDT" ? 140 : 620);
        const drift = seed * 0.0006;
        const randomDelta = (Math.random() - 0.5) * drift;
        const next = Math.max(0.0001, seed + randomDelta);

        this.onPriceTick({
          symbol,
          price: Number(next.toFixed(6)),
          ts: now,
          source: "fallback",
        });
      }
    }, 1000);
  }
}

module.exports = { MarketService };
