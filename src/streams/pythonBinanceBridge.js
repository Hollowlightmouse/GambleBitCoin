const path = require("path");
const readline = require("readline");
const { spawn } = require("child_process");
const { log, warn, error } = require("../config/logger");

class PythonBinanceBridge {
  constructor({ pythonCmd, symbols, onPrice, maxRestarts = 6, onFatal = null, apiKey = "", apiSecret = "", tld = "com", wsTimeout = 30 }) {
    this.pythonCmd = pythonCmd;
    this.symbols = symbols;
    this.onPrice = onPrice;
    this.maxRestarts = maxRestarts;
    this.onFatal = onFatal;
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
    this.tld = tld;
    this.wsTimeout = wsTimeout;
    this.child = null;
    this.restartDelay = 2000;
    this.restartCount = 0;
    this.stopped = false;
  }

  start() {
    const scriptPath = path.join(process.cwd(), "src", "streams", "binance_py_stream.py");

    const startProcess = () => {
      if (this.stopped) return;

      const env = {
        ...process.env,
        BINANCE_SYMBOLS: this.symbols.map((s) => s.toLowerCase()).join(","),
        BINANCE_API_KEY: this.apiKey,
        BINANCE_API_SECRET: this.apiSecret,
        BINANCE_TLD: this.tld,
        BINANCE_WS_TIMEOUT: String(this.wsTimeout),
      };

      this.child = spawn(this.pythonCmd, [scriptPath], {
        cwd: process.cwd(),
        env,
        stdio: ["ignore", "pipe", "pipe"],
      });

      log("py-binance", `started with ${this.pythonCmd}`);

      const rl = readline.createInterface({ input: this.child.stdout });
      rl.on("line", (line) => {
        try {
          const payload = JSON.parse(line);
          if (payload && payload.type === "stream_error") {
            warn("py-binance", payload.message || "stream_error");
            return;
          }
          if (!payload?.symbol || payload.price === undefined) return;
          if (this.restartCount > 0) {
            this.restartCount = 0;
            this.restartDelay = 2000;
          }
          this.onPrice({
            symbol: String(payload.symbol).toUpperCase(),
            price: Number(payload.price),
            ts: Number(payload.ts || Date.now()),
          });
        } catch (_err) {
          // ignore non-json lines
        }
      });

      this.child.stderr.on("data", (chunk) => {
        const message = chunk.toString().trim();
        if (message) warn("py-binance", message);
      });

      this.child.on("close", (code) => {
        if (this.stopped) return;

        this.restartCount += 1;
        if (this.restartCount > this.maxRestarts) {
          this.stopped = true;
          error("py-binance", `max restarts reached (${this.maxRestarts}), disabling python stream`);
          if (typeof this.onFatal === "function") {
            this.onFatal(new Error("python-binance stream unavailable"));
          }
          return;
        }

        warn("py-binance", `process closed with code ${code}, restarting in ${this.restartDelay}ms`);
        setTimeout(startProcess, this.restartDelay);
        this.restartDelay = Math.min(12000, this.restartDelay + 1000);
      });

      this.child.on("error", (err) => {
        error("py-binance", `spawn error: ${err.message}`);
      });
    };

    startProcess();
  }
}

module.exports = { PythonBinanceBridge };
