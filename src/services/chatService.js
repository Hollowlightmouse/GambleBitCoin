const { v4: uuidv4 } = require("uuid");
const { ChatMessage } = require("../models/ChatMessage");

class ChatService {
  constructor(repo) {
    this.repo = repo;
  }

  async postMessage({ symbol, user, text }) {
    const safeText = String(text || "").trim().slice(0, 300);
    if (!safeText) return null;

    const message = new ChatMessage({
      id: uuidv4(),
      symbol,
      userId: user.id,
      userName: user.name,
      text: safeText,
      ts: Date.now(),
    });

    await this.repo.saveChat(symbol, message);
    return message;
  }

  async getRecent(symbol) {
    return this.repo.getRecentChat(symbol);
  }
}

module.exports = { ChatService };
