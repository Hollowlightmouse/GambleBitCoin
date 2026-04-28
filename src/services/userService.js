const { User } = require("../models/User");

class UserService {
  constructor(repo, config) {
    this.repo = repo;
    this.config = config;
  }

  async findOrCreateUser(name) {
    const safeName = String(name || "").trim().slice(0, 20);
    if (!safeName) {
      throw new Error("Name is required");
    }

    // Buscar si el usuario ya existe por nombre
    let user = await this.repo.getUserByName(safeName);

    if (!user) {
      // Crear nuevo usuario con el nombre como ID
      user = new User(
        {
          id: safeName,
          name: safeName,
          balance: this.config.initialBalance,
          blocked: false,
          createdAt: Date.now(),
        },
        {
          betMin: this.config.betMin,
          betMax: this.config.betMax,
        }
      );
      await this.repo.saveUser(user);
    }

    return user;
  }

  async getUserByName(name) {
    return this.repo.getUserByName(name);
  }
}

module.exports = { UserService };
