function registerHealthController(app, { kafkaMirror, symbols, config }) {
  app.get("/health", async (_req, res) => {
    res.json({
      ok: true,
      storage: "memory",
      kafka: kafkaMirror.ready,
      symbols,
      roundSeconds: config.roundSeconds,
      lockSeconds: config.lockSeconds,
    });
  });
}

module.exports = { registerHealthController };
