function registerHealthController(app, { redis, kafkaMirror, symbols, config }) {
  app.get("/health", async (_req, res) => {
    let redisOk = false;
    try {
      const pong = await redis.ping();
      redisOk = pong === "PONG";
    } catch (_err) {
      redisOk = false;
    }

    res.json({
      ok: true,
      redis: redisOk,
      kafkaMirror: kafkaMirror.ready,
      symbols,
      roundSeconds: config.roundSeconds,
      lockSeconds: config.lockSeconds,
    });
  });
}

module.exports = { registerHealthController };
