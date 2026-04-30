// In-memory repository implementing same interface as RedisRepository
var MemoryRepository = function() {
  this.users = {};         // userId -> user object
  this.userNameToId = {};  // userName -> userId
  this.rounds = {};        // symbol -> round object (current)
  this.roundHistory = {};   // symbol -> array of round summaries
  this.bets = {};           // roundId:symbol:bets -> { userId -> bet object }
  this.chats = {};          // symbol -> array of chat messages
  this.leaderboards = {};   // key -> array of { id, name, score }
  this.globalLeaderboard = [];
};

// User operations
MemoryRepository.prototype.saveUser = function(user) {
  this.users[user.id] = {
    id: user.id,
    name: user.name,
    balance: user.balance,
    blocked: user.blocked,
    createdAt: user.createdAt
  };
  this.userNameToId[user.name] = user.id;
};

MemoryRepository.prototype.getUser = function(userId) {
  var user = this.users[userId];
  if (!user) return null;
  return this._createUserObject(user);
};

MemoryRepository.prototype.getUserByName = function(name) {
  var userId = this.userNameToId[name];
  if (!userId) return null;
  return this.getUser(userId);
};

// Helper to create user object with methods
MemoryRepository.prototype._createUserObject = function(user) {
  var obj = {
    id: user.id,
    name: user.name,
    balance: Number(user.balance),
    blocked: user.blocked === true,
    createdAt: Number(user.createdAt),
    rules: { betMin: 10, betMax: 300 },
    canBet: function(amount) {
      return (
        !this.blocked &&
        amount >= this.rules.betMin &&
        amount <= this.rules.betMax &&
        this.balance >= amount
      );
    },
    debit: function(amount) {
      this.balance -= amount;
      if (this.balance <= 0) {
        this.balance = 0;
        this.blocked = true;
      }
    },
    credit: function(amount) {
      this.balance += amount;
    }
  };
  return obj;
};

MemoryRepository.prototype.setUserBalanceAndStatus = function(userId, balance, blocked) {
  var user = this.users[userId];
  if (user) {
    user.balance = balance;
    user.blocked = blocked;
  }
};

// Round operations
MemoryRepository.prototype.saveRound = function(symbol, round) {
  this.rounds[symbol] = JSON.parse(JSON.stringify(round));
};

MemoryRepository.prototype.getCurrentRound = function(symbol) {
  return this.rounds[symbol] || null;
};

MemoryRepository.prototype.appendRoundHistory = function(symbol, summary) {
  var key = symbol;
  if (!this.roundHistory[key]) this.roundHistory[key] = [];
  this.roundHistory[key].unshift(JSON.parse(JSON.stringify(summary)));
  if (this.roundHistory[key].length > 100) this.roundHistory[key] = this.roundHistory[key].slice(0, 100);
};

// Bet operations
MemoryRepository.prototype.saveBet = function(bet) {
  var key = bet.roundId + ':' + bet.symbol + ':bets';
  if (!this.bets[key]) this.bets[key] = {};
  this.bets[key][bet.userId] = JSON.parse(JSON.stringify(bet));
};

MemoryRepository.prototype.getBetForUser = function(symbol, roundId, userId) {
  var key = roundId + ':' + symbol + ':bets';
  var bets = this.bets[key];
  if (!bets) return null;
  var raw = bets[userId];
  return raw ? JSON.parse(JSON.stringify(raw)) : null;
};

MemoryRepository.prototype.getRoundBets = function(symbol, roundId) {
  var key = roundId + ':' + symbol + ':bets';
  var bets = this.bets[key];
  if (!bets) return [];
  return Object.keys(bets).map(function(userId) { return JSON.parse(JSON.stringify(bets[userId])); });
};

MemoryRepository.prototype.deleteBet = function(symbol, roundId, userId) {
  var key = roundId + ':' + symbol + ':bets';
  if (this.bets[key]) delete this.bets[key][userId];
};

// Chat operations
MemoryRepository.prototype.saveChat = function(symbol, message) {
  var key = symbol;
  if (!this.chats[key]) this.chats[key] = [];
  this.chats[key].unshift(JSON.parse(JSON.stringify(message)));
  if (this.chats[key].length > 200) this.chats[key] = this.chats[key].slice(0, 200);
};

MemoryRepository.prototype.getRecentChat = function(symbol, limit) {
  var key = symbol;
  var chat = this.chats[key] || [];
  return chat.slice(0, limit || 50).map(function(msg) { return JSON.parse(JSON.stringify(msg)); }).reverse();
};

// Leaderboard operations
MemoryRepository.prototype.updateLeaderboards = function(symbol, userId, userName, balance) {
  var roomKey = 'leaderboard:' + symbol;
  if (!this.leaderboards[roomKey]) this.leaderboards[roomKey] = [];
  var roomBoard = this.leaderboards[roomKey];
  var found = false;
  for (var i = 0; i < roomBoard.length; i++) {
    if (roomBoard[i].id === userId) {
      roomBoard[i].score = balance;
      found = true;
      break;
    }
  }
  if (!found) roomBoard.push({ id: userId, name: userName, score: balance });

  // Global leaderboard
  var global = this.globalLeaderboard;
  found = false;
  for (var j = 0; j < global.length; j++) {
    if (global[j].id === userId) {
      global[j].score = balance;
      found = true;
      break;
    }
  }
  if (!found) global.push({ id: userId, name: userName, score: balance });
};

MemoryRepository.prototype.getLeaderboard = function(key, limit) {
  var board;
  if (key === 'leaderboard:global') {
    board = this.globalLeaderboard;
  } else {
    board = this.leaderboards[key] || [];
  }
  var sorted = board.slice().sort(function(a, b) { return b.score - a.score; });
  return sorted.slice(0, limit || 10).map(function(item) {
    return { id: item.id, name: item.name, score: item.score };
  });
};

module.exports = { MemoryRepository: MemoryRepository };
