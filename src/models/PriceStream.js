class PriceStream {
  constructor({ symbol, latestPrice = null, tickSize }) {
    this.symbol = symbol;
    this.latestPrice = latestPrice;
    this.tickSize = tickSize;
    this.currentRound = null;
  }

  setPrice(price) {
    this.latestPrice = Number(price);
  }

  setRound(round) {
    this.currentRound = round;
  }
}

module.exports = { PriceStream };
