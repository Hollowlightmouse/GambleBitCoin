const { User } = require("../models/User");

class RedisRepository {
  constructor(redis, config) {
    this.redis = redis;
    this.config = config;
  }

  userKey(userId) {
    return `user:${userId}`;
  }

  userNameIndexKey(name) {
    return `username:${name}`;
  }

  roundCurrentKey(symbol) {
    return `round:${symbol}:current`;
  }

  roundHistoryKey(symbol) {
    return `round:${symbol}:history`;
  }

  betsHashKey(symbol, roundId) {
    return `round:${roundId}:${symbol}:bets`;
  }

  speedKey(symbol, roundId, side) {
    return `round:${roundId}:${symbol}:speed:${side}`;
  }

  chatKey(symbol) {
    return `chat:${symbol}:messages`;
  }

  leaderboardKey(symbol) {
    return `leaderboard:${symbol}`;
  }

  leaderboardGlobalKey() {
    return "leaderboard:global";
  }

  async saveUser(user) {
    await this.redis.hset(this.userKey(user.id), {
      id: user.id,
      name: user.name,
      balance: String(user.balance),
      blocked: String(user.blocked),
      createdAt: String(user.createdAt),
    });
    // Guardar índice de nombre único
    await this.redis.set(this.userNameIndexKey(user.name), user.id);
  }

  async getUser(userId) {
    const raw = await this.redis.hgetall(this.userKey(userId));
    if (!raw || !raw.id) return null;
    return new User(raw, { betMin: this.config.betMin, betMax: this.config.betMax });
  }

  async getUserByName(name) {
    const userId = await this.redis.get(this.userNameIndexKey(name));
    if (!userId) return null;
    return this.getUser(userId);
  }

  async deleteUser(userId) {
    const user = await this.getUser(userId);
    if (user) {
      await this.redis.del(this.userNameIndexKey(user.name));
    }
    await this.redis.del(this.userKey(userId));
  }

  async setUserBalanceAndStatus(userId, balance, blocked) {
    await this.redis.hset(this.userKey(userId), {
      balance: String(balance),
      blocked: String(blocked),
    });
  }

  async saveRound(symbol, round) {
    await this.redis.set(this.roundCurrentKey(symbol), JSON.stringify(round));
  }

  async getCurrentRound(symbol) {
    const raw = await this.redis.get(this.roundCurrentKey(symbol));
    if (!raw) return null;
    return JSON.parse(raw);
  }

  async appendRoundHistory(symbol, summary) {
    const key = this.roundHistoryKey(symbol);
    await this.redis.lpush(key, JSON.stringify(summary));
    await this.redis.ltrim(key, 0, this.config.roundHistoryLimit - 1);
  }

  async saveBet(bet) {
    const key = this.betsHashKey(bet.symbol, bet.roundId);
    await this.redis.hset(key, bet.userId, JSON.stringify(bet));
    await this.redis.zadd(this.speedKey(bet.symbol, bet.roundId, bet.side), bet.timestamp, bet.userId);
  }

  async getBetForUser(symbol, roundId, userId) {
    const raw = await this.redis.hget(this.betsHashKey(symbol, roundId), userId);
    return raw ? JSON.parse(raw) : null;
  }

  async getRoundBets(symbol, roundId) {
    const rows = await this.redis.hgetall(this.betsHashKey(symbol, roundId));
    return Object.values(rows).map((value) => JSON.parse(value));
  }

  async deleteBet(symbol, roundId, userId) {
    await this.redis.hdel(this.betsHashKey(symbol, roundId), userId);
  }

  async saveChat(symbol, message) {
    const key = this.chatKey(symbol);
    await this.redis.lpush(key, JSON.stringify(message));
    await this.redis.ltrim(key, 0, this.config.chatLimit - 1);
  }

  async getRecentChat(symbol, limit = 50) {
    const rows = await this.redis.lrange(this.chatKey(symbol), 0, limit - 1);
    return rows.map((value) => JSON.parse(value)).reverse();
  }

  async updateLeaderboards(symbol, userId, userName, balance) {
    await this.redis.zadd(this.leaderboardKey(symbol), Number(balance), `${userId}|${userName}`);
    await this.redis.zadd(this.leaderboardGlobalKey(), Number(balance), `${userId}|${userName}`);
  }

  async getLeaderboard(key, limit = 10) {
    const rows = await this.redis.zrevrange(key, 0, limit - 1, "WITHSCORES");
    const output = [];
    for (let i = 0; i < rows.length; i += 2) {
      const value = rows[i] || "";
      const score = Number(rows[i + 1] || 0);
      const [id, name] = value.split("|");
      output.push({ id, name, score });
    }
    return output;
  }
}

module.exports = { RedisRepository };
