const { SYMBOLS } = require("../config/constants");

function registerUserController(app, { userService, marketService, chatService, leaderboardService }) {
  app.post("/api/users", async (req, res) => {
    try {
      const { userId, name } = req.body || {};
      const user = await userService.findOrCreateUser(userId, name);
      res.json({
        user: {
          id: user.id,
          name: user.name,
          balance: user.balance,
          blocked: user.blocked,
        },
        symbols: SYMBOLS,
      });
    } catch (err) {
      res.status(400).json({ error: err.message });
    }
  });

  app.get("/api/users/:id", async (req, res) => {
    const user = await userService.getUserById(req.params.id);
    if (!user) {
      res.status(404).json({ error: "User not found" });
      return;
    }
    res.json({
      id: user.id,
      name: user.name,
      balance: user.balance,
      blocked: user.blocked,
      createdAt: user.createdAt,
    });
  });

  app.get("/api/markets/:symbol/state", async (req, res) => {
    const { symbol } = req.params;
    if (!SYMBOLS.includes(symbol)) {
      res.status(400).json({ error: "Invalid symbol" });
      return;
    }

    const state = marketService.getState(symbol);
    const boards = await leaderboardService.getBoards(symbol);
    const chat = await chatService.getRecent(symbol);
    res.json({
      symbol,
      latestPrice: state.latestPrice,
      tickSize: state.tickSize,
      currentRound: state.currentRound,
      roomBoard: boards.roomBoard,
      globalBoard: boards.globalBoard,
      chat,
    });
  });
}

module.exports = { registerUserController };
