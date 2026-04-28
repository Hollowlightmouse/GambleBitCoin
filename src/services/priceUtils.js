function roundByTickSize(value, tickSize) {
  if (!tickSize || tickSize <= 0) return value;
  return Math.round(value / tickSize) * tickSize;
}

function determineResult(startPrice, endPrice, tickSize) {
  const startNorm = roundByTickSize(startPrice, tickSize);
  const endNorm = roundByTickSize(endPrice, tickSize);
  if (endNorm > startNorm) return "up";
  if (endNorm < startNorm) return "down";
  return "hold";
}

module.exports = {
  roundByTickSize,
  determineResult,
};
