const path = require("path");
const readline = require("readline");
const { spawn } = require("child_process");
const { log, warn, error } = require("../config/logger");

class BinanceKafkaProducer {
  constructor({ pythonCmd, symbols, kafkaMirror, topic, maxRestarts = 6, onFatal = null }) {
    this.pythonCmd = pythonCmd;
    this.symbols = symbols;
    this.kafkaMirror = kafkaMirror;
    this.topic = topic;
    this.maxRestarts = maxRestarts;
    this.onFatal = onFatal;
    this.child = null;
    this.restartDelay = 2000;
    this.restartCount = 0;
    this.stopped = false;
  }

  start() {
    const scriptPath = path.join(process.cwd(), "src", "streams", "binance_kafka_producer.py");

    const startProcess = () => {
      if (this.stopped) return;

      const env = {
        ...process.env,
        BINANCE_SYMBOLS: this.symbols.map((s) => s.toLowerCase()).join(","),
      };

      this.child = spawn(this.pythonCmd, [scriptPath], {
        cwd: process.cwd(),
        env,
        stdio: ["ignore", "pipe", "pipe"],
      });

      log("py-binance", `started with ${this.pythonCmd}`);

      const rl = readline.createInterface({ input: this.child.stdout });
      rl.on("line", (line) => {
        if (!line) return;
        if (this.restartCount > 0) {
          this.restartCount = 0;
          this.restartDelay = 2000;
        }
        log("py-binance", line.trim());
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

module.exports = { BinanceKafkaProducer };
