class Leaderboard {
  constructor({ roomBoard = [], globalBoard = [] } = {}) {
    this.roomBoard = roomBoard;
    this.globalBoard = globalBoard;
  }
}

module.exports = { Leaderboard };
