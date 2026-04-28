const SYMBOLS = ["ETHUSDT", "SOLUSDT", "BNBUSDT"];
const SIDES = ["up", "down", "hold"];

const DEFAULT_TICK_SIZE = {
  ETHUSDT: 0.01,
  SOLUSDT: 0.001,
  BNBUSDT: 0.01,
};

module.exports = {
  SYMBOLS,
  SIDES,
  DEFAULT_TICK_SIZE,
};
