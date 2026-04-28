const { SYMBOLS } = require("../config/constants");

function registerSocketHandlers(io, services) {
  const {
    userService,
    marketService,
    betService,
    chatService,
    leaderboardService,
    repo,
  } = services;

  function room(symbol) {
    return `room:${symbol}`;
  }

  io.on("connection", (socket) => {
    socket.data.userId = null;
    socket.data.market = null;

    socket.on("join_user", async ({ name }) => {
      try {
        const user = await userService.findOrCreateUser(name);
        socket.data.userId = user.id;
        socket.emit("user_joined", {
          user: {
            id: user.id,
            name: user.name,
            balance: user.balance,
            blocked: user.blocked,
          },
          symbols: SYMBOLS,
        });
      } catch (err) {
        socket.emit("error_message", { message: err.message || "Could not join user" });
      }
    });

    socket.on("join_market", async ({ symbol }) => {
      try {
        if (!socket.data.userId) {
          socket.emit("error_message", { message: "Join user first" });
          return;
        }

        if (!SYMBOLS.includes(symbol)) {
          socket.emit("error_message", { message: "Invalid market" });
          return;
        }

        if (socket.data.market) {
          socket.leave(room(socket.data.market));
        }

        socket.data.market = symbol;
        socket.join(room(symbol));

        const user = await repo.getUser(socket.data.userId);
        const market = marketService.getState(symbol);
        const boards = await leaderboardService.getBoards(symbol);
        const chat = await chatService.getRecent(symbol);

        socket.emit("market_joined", {
          symbol,
          user: {
            id: user.id,
            name: user.name,
            balance: user.balance,
            blocked: user.blocked,
          },
          latestPrice: market.latestPrice,
          round: market.currentRound,
          roomBoard: boards.roomBoard,
          globalBoard: boards.globalBoard,
          chat,
        });

        io.to(room(symbol)).emit("user_action", {
          symbol,
          message: `${user.name} joined ${symbol}`,
          ts: Date.now(),
        });
      } catch (_err) {
        socket.emit("error_message", { message: "Could not join market" });
      }
    });

    socket.on("place_bet", async ({ symbol, side, amount }) => {
      try {
        if (!socket.data.userId) return;
        if (socket.data.market !== symbol) {
          socket.emit("bet_rejected", { message: "Join this market first" });
          return;
        }

        const { bet, user } = await betService.placeBet({
          userId: socket.data.userId,
          symbol,
          side,
          amount,
        });

        socket.emit("bet_accepted", {
          bet,
          balance: user.balance,
          blocked: user.blocked,
        });

        io.to(room(symbol)).emit("user_action", {
          symbol,
          message: `${user.name} placed a ${side} bet (${amount})`,
          ts: Date.now(),
        });

        if (user.blocked) {
          socket.emit("user_blocked", {
            message: "Has perdido por completo. No puedes seguir apostando.",
            balance: user.balance,
          });
        }

        const boards = await leaderboardService.getBoards(symbol);
        io.to(room(symbol)).emit("leaderboard_updated", {
          symbol,
          roomBoard: boards.roomBoard,
          globalBoard: boards.globalBoard,
        });
      } catch (err) {
        if (err.code === "USER_BLOCKED") {
          const user = await repo.getUser(socket.data.userId);
          socket.emit("user_blocked", {
            message: err.message,
            balance: user ? user.balance : 0,
          });
          return;
        }

        socket.emit("bet_rejected", { message: err.message || "Could not place bet" });
      }
    });

    socket.on("send_chat_message", async ({ symbol, text }) => {
      try {
        if (!socket.data.userId) return;
        if (socket.data.market !== symbol) return;

        const user = await repo.getUser(socket.data.userId);
        if (!user) return;

        const message = await chatService.postMessage({ symbol, user, text });
        if (!message) return;

        io.to(room(symbol)).emit("chat_message", message);
      } catch (_err) {
        // no-op
      }
    });
  });
}

module.exports = { registerSocketHandlers };
