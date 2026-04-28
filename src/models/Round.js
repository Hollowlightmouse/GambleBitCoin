class Round {
  constructor({ id, symbol, startPrice, startAt, endAt, lockAt }) {
    this.id = id;
    this.symbol = symbol;
    this.startPrice = Number(startPrice);
    this.endPrice = null;
    this.startAt = Number(startAt);
    this.endAt = Number(endAt);
    this.lockAt = Number(lockAt);
    this.locked = false;
    this.status = "open";
    this.result = null;
  }
}

module.exports = { Round };
