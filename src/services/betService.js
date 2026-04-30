const { v4: uuidv4 } = require("uuid");
const { Bet } = require("../models/Bet");

class BetService {
  constructor({ repo, config, symbols, sides, marketService, leaderboardService, kafkaMirror }) {
    this.repo = repo;
    this.config = config;
    this.symbols = symbols;
    this.sides = sides;
    this.marketService = marketService;
    this.leaderboardService = leaderboardService;
    this.kafkaMirror = kafkaMirror;
  }

  async placeBet({ userId, symbol, side, amount }) {
    if (!this.symbols.includes(symbol)) {
      throw new Error("Invalid market");
    }
    if (!this.sides.includes(side)) {
      throw new Error("Invalid side");
    }

    const round = this.marketService.getCurrentRound(symbol);
    if (!round || round.status !== "open") {
      throw new Error("Round is not open");
    }
    if (round.locked || Date.now() >= round.lockAt) {
      throw new Error("Bet lock is active (last 5s)");
    }

    const numericAmount = Number(amount);
    const user = await this.repo.getUser(userId);
    if (!user) throw new Error("User not found");

    if (user.blocked) {
      const blockedError = new Error("Has perdido por completo. No puedes seguir apostando.");
      blockedError.code = "USER_BLOCKED";
      throw blockedError;
    }

    if (!user.canBet(numericAmount)) {
      throw new Error(
        `Invalid bet. Amount range is ${this.config.betMin}-${this.config.betMax} and enough balance is required.`
      );
    }

    const existing = await this.repo.getBetForUser(symbol, round.id, user.id);
    if (existing) {
      throw new Error("Only one bet per round");
    }

    const bet = new Bet({
      id: uuidv4(),
      roundId: round.id,
      symbol,
      userId: user.id,
      userName: user.name,
      side,
      amount: numericAmount,
      timestamp: Date.now(),
    });

    user.debit(numericAmount);

    await this.repo.saveBet(bet);
    await this.repo.setUserBalanceAndStatus(user.id, user.balance, user.blocked);
    await this.leaderboardService.updateForUser(symbol, user);

    await this.kafkaMirror.send(this.config.kafkaBetTopic, {
      type: "bet_placed",
      ts: Date.now(),
      symbol,
      roundId: round.id,
      userId: user.id,
      userName: user.name,
      side,
      amount: numericAmount,
      balanceAfter: user.balance,
    });

    return {
      bet,
      user,
    };
  }
}

module.exports = { BetService };
