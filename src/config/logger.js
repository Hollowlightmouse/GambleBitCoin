function log(scope, ...args) {
  console.log(`[${scope}]`, ...args);
}

function warn(scope, ...args) {
  console.warn(`[${scope}]`, ...args);
}

function error(scope, ...args) {
  console.error(`[${scope}]`, ...args);
}

module.exports = {
  log,
  warn,
  error,
};
