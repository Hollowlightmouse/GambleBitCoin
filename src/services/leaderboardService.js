class LeaderboardService {
  constructor(repo) {
    this.repo = repo;
  }

  async getBoards(symbol) {
    const roomKey = 'leaderboard:' + symbol;
    const globalKey = 'leaderboard:global';
    const roomBoard = await this.repo.getLeaderboard(roomKey, 10);
    const globalBoard = await this.repo.getLeaderboard(globalKey, 10);
    return { roomBoard, globalBoard };
  }

  async updateForUser(symbol, user) {
    await this.repo.updateLeaderboards(symbol, user.id, user.name, user.balance);
  }
}

module.exports = { LeaderboardService };
