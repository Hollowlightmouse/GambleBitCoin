class LeaderboardService {
  constructor(repo) {
    this.repo = repo;
  }

  async getBoards(symbol) {
    const roomBoard = await this.repo.getLeaderboard(this.repo.leaderboardKey(symbol), 10);
    const globalBoard = await this.repo.getLeaderboard(this.repo.leaderboardGlobalKey(), 10);
    return { roomBoard, globalBoard };
  }

  async updateForUser(symbol, user) {
    await this.repo.updateLeaderboards(symbol, user.id, user.name, user.balance);
  }
}

module.exports = { LeaderboardService };
