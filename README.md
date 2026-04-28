# 🎲 Streaming Prediction Market

A real-time cryptocurrency prediction market built with Node.js, Socket.IO, Redis, and Binance API integration. Users predict price movements (UP, DOWN, or HOLD) in 20-second rounds and compete on a leaderboard.

## 🚀 Quick Start

### Prerequisites
- Node.js 16+
- Redis (running on `localhost:6379`)
- Kafka (optional, running on `localhost:9092`)
- Python 3.13 (for Binance stream)

### Installation

```bash
# Clone repository
git clone <repo-url>
cd GambleBitCoin

# Install dependencies
npm install

# Configure environment
cp .env.example .env
# Edit .env with your Binance API credentials
```

### Environment Variables

Create a `.env` file:

```env
PORT=3000
REDIS_URL=redis://localhost:6379
KAFKA_BROKERS=localhost:9092

# Binance API (optional)
BINANCE_API_KEY=your_key_here
BINANCE_API_SECRET=your_secret_here

# Game config
ROUND_SECONDS=20
LOCK_SECONDS=5
INITIAL_BALANCE=2000
BET_MIN=10
BET_MAX=300
CHAT_LIMIT=100

# Features
USE_PY_BINANCE=true
ENABLE_PRICE_FALLBACK=true
KAFKA_ENABLED=true
```

### Start

```bash
npm start
```

The app will open automatically at `http://localhost:3000`

## 🎮 Game Rules

### Round Flow
- **20 seconds**: Round is open for betting
- **15 seconds**: Normal betting phase
- **5 seconds**: Betting lock activated (cannot place bets)
- **End**: Round settles, payouts calculated

### Betting
- **UP**: Price will go higher than start price
- **DOWN**: Price will go lower than start price
- **HOLD**: Price will stay the same (±tick size)
- Min bet: 10 créditos
- Max bet: 300 créditos
- One bet per user per round

### Payouts
- **Base**: `bet_amount × 2`
- **Speed bonus**:
  - 1st winner: `×1.08` (8% bonus)
  - 2nd winner: `×1.06` (6% bonus)
- **Tie (HOLD wins)**: When start price equals end price (after rounding)

### User Status
- Initial balance: 2000 créditos
- Balance ≤ 0: User blocked (cannot bet)
- Unique usernames: Each name is persistent (reload to recover session)

## 📊 Markets Available

- **ETHUSDT** - Ethereum
- **SOLUSDT** - Solana
- **BNBUSDT** - Binance Coin

## 🏗️ Architecture

```
├── app.js                 # Main entry point
├── src/
│   ├── config/
│   │   ├── constants.js   # Market symbols, bet sides
│   │   └── env.js         # Environment configuration
│   ├── models/            # Data models (User, Bet, Round, etc)
│   ├── repositories/      # Redis CRUD operations
│   ├── services/          # Business logic
│   ├── controllers/       # HTTP endpoints
│   ├── sockets/           # Socket.IO handlers
│   ├── streams/           # Binance price streams
│   └── public/            # Frontend (HTML, CSS, JS)
└── package.json
```

## 🔌 API Endpoints

### Health
```
GET /health
```

### Users
```
GET  /users/:userId
POST /users
PATCH /users/:userId
```

### Admin (if enabled)
```
POST /admin/balance
POST /admin/round/start
POST /admin/round/close
```

## 🔄 Socket.IO Events

### Client → Server
- `join_user`: Login with username
- `join_market`: Join market room (ETHUSDT, SOLUSDT, BNBUSDT)
- `place_bet`: Place a bet (side, amount)
- `send_chat_message`: Send chat message

### Server → Client
- `user_joined`: User successfully logged in
- `market_joined`: Joined market room (with initial state)
- `price_tick`: New price received
- `round_started`: New round begins
- `round_timer`: Timer update (every second)
- `round_locked`: Betting locked
- `round_ended`: Round settled with payouts
- `leaderboard_updated`: Leaderboard changed
- `chat_message`: New chat message
- `user_action`: User activity (join, bet, etc)
- `bet_accepted`: Bet confirmed
- `bet_rejected`: Bet failed
- `user_blocked`: User balance ≤ 0

## 📱 Responsive Design

The UI scales perfectly on:
- **Desktop** (1200px+): 2-column layout
- **Tablets** (800-1200px): 1-column layout
- **Mobile** (480-800px): Compact layout
- **Small mobile** (<480px): Ultra-compact

Uses CSS `clamp()` for fluid scaling without media query jumps.

## 💾 Data Storage

### Redis
- User sessions & balances: `user:{name}`
- Round state: `round:{symbol}:current`
- Bets: `round:{roundId}:{symbol}:bets`
- Chat: `chat:{symbol}:messages`
- Leaderboards: `leaderboard:{symbol}` (room), `leaderboard:global`

### Kafka (Optional)
- Price updates: `market.prices.raw`
- Bet events: `market.bets.events`
- Round events: `market.round.events`

## 🚦 Features

✅ Real-time price streaming from Binance
✅ WebSocket multiplayer (Socket.IO)
✅ Persistent user sessions (by username)
✅ Leaderboard (room-based)
✅ Live chat
✅ Responsive UI (mobile, tablet, desktop)
✅ Fallback price generator (if Binance stalls)
✅ Kafka event audit trail
✅ Speed bonus payouts
✅ Auto-open browser on startup

## 🐛 Development

### Run Tests
```bash
npm test
```

### Rebuild Chart.js
```bash
npm run build
```

### View Logs
```bash
# Kafka
docker logs kafka-kafka-1 -f

# Redis
docker logs mi-redis -f
```

## 📝 License

MIT

## 🤝 Support

For issues or questions, open an issue on GitHub.
