const { SYMBOLS } = require("../config/constants");

function registerAdminController(app, { repo, marketService, config }) {
  app.patch("/api/users/:id/balance", async (req, res) => {
    const user = await repo.getUser(req.params.id);
    if (!user) {
      res.status(404).json({ error: "User not found" });
      return;
    }

    const balance = Number(req.body?.balance);
    if (!Number.isFinite(balance) || balance < 0) {
      res.status(400).json({ error: "Invalid balance" });
      return;
    }

    user.balance = balance;
    user.blocked = balance <= 0;
    await repo.setUserBalanceAndStatus(user.id, user.balance, user.blocked);

    res.json({
      id: user.id,
      name: user.name,
      balance: user.balance,
      blocked: user.blocked,
    });
  });

  app.delete("/api/users/:id", async (req, res) => {
    await repo.deleteUser(req.params.id);
    res.json({ ok: true });
  });

  app.post("/api/bets", async (req, res) => {
    const { symbol, userId, side, amount } = req.body || {};
    if (!SYMBOLS.includes(symbol)) {
      res.status(400).json({ error: "Invalid symbol" });
      return;
    }

    const round = marketService.getCurrentRound(symbol);
    if (!round) {
      res.status(409).json({ error: "Round not active" });
      return;
    }

    const existing = await repo.getBetForUser(symbol, round.id, userId);
    if (existing) {
      res.status(409).json({ error: "Bet already exists for this round" });
      return;
    }

    const numericAmount = Number(amount);
    if (!Number.isFinite(numericAmount) || numericAmount < config.betMin || numericAmount > config.betMax) {
      res.status(400).json({ error: "Invalid amount" });
      return;
    }

    if (!["up", "down", "hold"].includes(side)) {
      res.status(400).json({ error: "Invalid side" });
      return;
    }

    const user = await repo.getUser(userId);
    if (!user) {
      res.status(404).json({ error: "User not found" });
      return;
    }

    user.debit(numericAmount);
    await repo.setUserBalanceAndStatus(user.id, user.balance, user.blocked);

    const bet = {
      id: `${Date.now()}-${Math.random()}`,
      roundId: round.id,
      symbol,
      userId: user.id,
      userName: user.name,
      side,
      amount: numericAmount,
      timestamp: Date.now(),
    };

    await repo.saveBet(bet);
    res.json({ bet, user: { id: user.id, balance: user.balance, blocked: user.blocked } });
  });

  app.get("/api/bets", async (req, res) => {
    const { symbol, roundId } = req.query;
    if (!SYMBOLS.includes(symbol)) {
      res.status(400).json({ error: "Invalid symbol" });
      return;
    }
    if (!roundId) {
      res.status(400).json({ error: "roundId is required" });
      return;
    }

    const bets = await repo.getRoundBets(symbol, roundId);
    res.json({ symbol, roundId, bets });
  });

  app.delete("/api/bets", async (req, res) => {
    const { symbol, roundId, userId } = req.query;
    if (!SYMBOLS.includes(symbol) || !roundId || !userId) {
      res.status(400).json({ error: "symbol, roundId and userId are required" });
      return;
    }

    await repo.deleteBet(symbol, roundId, userId);
    res.json({ ok: true });
  });

  app.post("/api/rounds/start", async (req, res) => {
    const { symbol } = req.body || {};
    if (!SYMBOLS.includes(symbol)) {
      res.status(400).json({ error: "Invalid symbol" });
      return;
    }
    await marketService.openRound(symbol);
    res.json({ ok: true, round: marketService.getCurrentRound(symbol) });
  });

  app.post("/api/rounds/close", async (req, res) => {
    const { symbol } = req.body || {};
    if (!SYMBOLS.includes(symbol)) {
      res.status(400).json({ error: "Invalid symbol" });
      return;
    }
    await marketService.settleRound(symbol);
    res.json({ ok: true });
  });
}

module.exports = { registerAdminController };
