class ChatMessage {
  constructor({ id, symbol, userId, userName, text, ts }) {
    this.id = id;
    this.symbol = symbol;
    this.userId = userId;
    this.userName = userName;
    this.text = text;
    this.ts = Number(ts);
  }
}

module.exports = { ChatMessage };
