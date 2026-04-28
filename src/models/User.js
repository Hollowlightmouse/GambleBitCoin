class User {
  constructor({ id, name, balance, blocked, createdAt }, rules) {
    this.id = id;
    this.name = name;
    this.balance = Number(balance);
    this.blocked = blocked === true || blocked === "true";
    this.createdAt = Number(createdAt);
    this.rules = rules;
  }

  canBet(amount) {
    return (
      !this.blocked &&
      amount >= this.rules.betMin &&
      amount <= this.rules.betMax &&
      this.balance >= amount
    );
  }

  debit(amount) {
    this.balance -= amount;
    if (this.balance <= 0) {
      this.balance = 0;
      this.blocked = true;
    }
  }

  credit(amount) {
    this.balance += amount;
  }
}

module.exports = { User };
