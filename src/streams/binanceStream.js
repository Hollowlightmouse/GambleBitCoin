const WebSocket = require("ws");
const { warn, error, log } = require("../config/logger");

class BinanceStream {
  constructor(symbols, onPrice, tld = "com") {
    this.symbols = symbols;
    this.onPrice = onPrice;
    this.tld = tld;
    this.ws = null;
    this.reconnectDelay = 2000;
  }

  start() {
    const streamPath = this.symbols.map((symbol) => `${symbol.toLowerCase()}@trade`).join("/");
    const url = `wss://stream.binance.${this.tld}:9443/stream?streams=${streamPath}`;

    const connect = () => {
      this.ws = new WebSocket(url);

      this.ws.on("open", () => {
        this.reconnectDelay = 2000;
        log("binance", "stream connected");
      });

      this.ws.on("message", (raw) => {
        try {
          const message = JSON.parse(raw.toString());
          const data = message.data;
          if (!data || !data.s) return;
          this.onPrice({
            symbol: data.s,
            price: Number(data.p),
            ts: Number(data.T || Date.now()),
          });
        } catch (err) {
          error("binance", `parse error: ${err.message}`);
        }
      });

      this.ws.on("pong", () => {
        // keepalive acknowledged
      });

      this.ws.on("close", () => {
        warn("binance", `stream closed, reconnect in ${this.reconnectDelay}ms`);
        setTimeout(connect, this.reconnectDelay);
        this.reconnectDelay = Math.min(12000, this.reconnectDelay + 1000);
      });

      this.ws.on("error", (err) => {
        error("binance", `stream error: ${err.message}`);
        try {
          this.ws.close();
        } catch (_ignore) {
          // no-op
        }
      });
    };

    connect();

    setInterval(() => {
      try {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.ws.ping();
        }
      } catch (_err) {
        // no-op
      }
    }, 15000);
  }
}

module.exports = { BinanceStream };
