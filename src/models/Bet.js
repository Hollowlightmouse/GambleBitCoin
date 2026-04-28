class Bet {
  constructor({ id, roundId, symbol, userId, userName, side, amount, timestamp }) {
    this.id = id;
    this.roundId = roundId;
    this.symbol = symbol;
    this.userId = userId;
    this.userName = userName;
    this.side = side;
    this.amount = Number(amount);
    this.timestamp = Number(timestamp);
  }
}

module.exports = { Bet };
