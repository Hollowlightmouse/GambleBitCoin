# 🎲 Streaming Prediction Market

Una aplicación real-time de mercado de predicción de criptomonedas construida con Node.js, Socket.IO, Redis e integración de Binance API. Los usuarios predicen movimientos de precios (UP, DOWN, o HOLD) en rondas de 20 segundos y compiten en un leaderboard.

## 🎮 ¿Cómo Funciona el Juego?

### Flujo de Juego

#### 1️⃣ **Login**
```
Usuario entra a http://localhost:3000
    ↓
Ingresa nombre de usuario (único y persistente)
    ↓
Selecciona mercado (ETHUSDT, SOLUSDT, o BNBUSDT)
    ↓
Presiona "Join Room"
    ↓
✅ Usuario conectado con 2000 créditos iniciales
```

#### 2️⃣ **Ronda (20 segundos)**

```
┌─────────────────────────────────────────────────────────┐
│ RONDA EN VIVO - 20 segundos                              │
├─────────────────────────────────────────────────────────┤
│                                                           │
│ 20-15s: Ronda abierta - PUEDES APOSTAR                  │
│         Precio: $3,245.50                                │
│         Timer: 20s ← Normal (verde)                      │
│                                                           │
│         [UP 50 créditos] [DOWN 50] [HOLD 50]            │
│         → Apuesta aceptada ✓                             │
│                                                           │
│ 14-11s: Ronda abierta - Esperando final                 │
│         Timer: 14s ← Normal                              │
│                                                           │
│ 10-6s:  Ronda abierta - Timer AMARILLO                  │
│         Timer: 10s ← Amarillo (⚠️ apresúrate)            │
│                                                           │
│ 5-0s:   BLOQUEO DE APUESTAS - Rojo                      │
│         Timer: 5s ← ROJO (🔴 no puedes apostar)         │
│         Botones deshabilitados                           │
│         "Bet lock active (last 5s)"                      │
│                                                           │
│ 0s:     RONDA TERMINA                                    │
│         Precio final: $3,248.75                          │
│         Resultado: UP ✓                                  │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

### 📊 Opciones de Apuesta

| Opción | Descripción | Gana Cuando |
|--------|-------------|-------------|
| **UP** 📈 | Precio sube | `Precio Final > Precio Inicial` |
| **DOWN** 📉 | Precio baja | `Precio Final < Precio Inicial` |
| **HOLD** ⏸️ | Precio se mantiene | `Precio Final = Precio Inicial` (±tick size) |

### 💰 Sistema de Payouts

```
Apuesta: 50 créditos
Ganadoras por opción: 10 usuarios

Payout base para ganadores:
└─ 50 × 2 = 100 créditos

Bonus por velocidad (top 2):
├─ 1️⃣ Primero en apostar: 100 × 1.08 = 108 créditos (+8%)
├─ 2️⃣ Segundo en apostar:  100 × 1.06 = 106 créditos (+6%)
└─ 3️⃣+ Resto:              100 créditos (sin bonus)

Perdedores: Pierden los créditos apostados
Empate (HOLD): Solo ganadores HOLD reciben payout
```

### 👤 Sistema de Usuarios

```
Primer Login:
└─ Nombre: "juan" → ID único persistente
└─ Balance: 2000 créditos iniciales
└─ Guardado en localStorage: pm_user_name = "juan"

Reconexión:
└─ Nombre pre-rellenado automáticamente
└─ Recupera su balance anterior (sesión persistente)
└─ UUID fue reemplazado por nombre como identificador

Bloqueo de Usuario:
└─ Balance ≤ 0 → Usuario bloqueado
└─ Mensaje: "Has perdido por completo. No puedes seguir apostando."
└─ Botones de apuesta deshabilitados
```

### 📈 Gráfico en Tiempo Real

```
Características:
├─ Actualización: Cada 5 segundos
├─ Puntos almacenados: 120 (10 minutos de datos)
├─ Colores dinámicos:
│  ├─ Verde 🟢: Precio subiendo
│  ├─ Rojo 🔴: Precio bajando
│  └─ Amarillo 🟡: Precio estable
├─ Interpolación suave (Bezier curves)
└─ Responsive: Se adapta a cualquier tamaño de pantalla
```

### 💬 Chat en Vivo

```
Características:
├─ Auto-scroll: Se queda al final automáticamente
├─ Scroll manual: Muestra botón "Latest" para volver al final
├─ Mensajes instantáneos: Socket.IO en tiempo real
├─ Eventos del sistema: Mostramos "usuario X se unió"
└─ Historial: Últimos 100 mensajes
```

### 🏆 Leaderboard (Sala)

```
TOP 10 POR SALA:
#1  john        - $5,234.50
#2  maria       - $4,892.00
#3  pedro       - $3,456.75
#4  ana         - $2,100.00
...
```

## 🚀 Quick Start

### Prerequisitos
- **Node.js** 16+
- **Redis** corriendo en `localhost:6379`
- **Kafka** (opcional) en `localhost:9092`
- **Python** 3.13 (para Binance stream)

### Instalación

```bash
# Clonar repositorio
git clone https://github.com/Hollowlightmouse/GambleBitCoin.git
cd GambleBitCoin

# Instalar dependencias
npm install

# Configurar .env
cp .env.example .env
# Editar .env con credenciales de Binance
```

### Variables de Entorno (`.env`)

```env
# Puerto
PORT=3000

# Redis
REDIS_URL=redis://localhost:6379

# Kafka (opcional)
KAFKA_BROKERS=localhost:9092

# Binance API
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
USE_PY_BINANCE=true

# Configuración del Juego
ROUND_SECONDS=20          # Duración de ronda en segundos
LOCK_SECONDS=5            # Últimos 5 segundos bloqueados
INITIAL_BALANCE=2000      # Saldo inicial
BET_MIN=10                # Apuesta mínima
BET_MAX=300               # Apuesta máxima
ROUND_HISTORY_LIMIT=100   # Historial guardado

# Chat
CHAT_LIMIT=100            # Últimos 100 mensajes

# Características
ENABLE_PRICE_FALLBACK=true   # Generador sintético si Binance falla
KAFKA_ENABLED=true            # Auditoría de eventos
```

### Iniciar

```bash
npm start
```

Si quieres levantar contenedores (Redis + app en Docker):

```bash
npm run start:docker
```

El navegador se abrirá automáticamente en `http://localhost:3000`

## 🏗️ Arquitectura del Sistema

```
CLIENT (Browser)
    ↓ Socket.IO
    ├─ join_user { name }
    ├─ join_market { symbol }
    ├─ place_bet { side, amount }
    └─ send_chat_message { text }
    ↑
    ├─ user_joined { user }
    ├─ market_joined { symbol, user, round, chat }
    ├─ price_tick { price }
    ├─ round_started / round_timer / round_locked / round_ended
    ├─ leaderboard_updated
    ├─ chat_message / user_action
    └─ bet_accepted / bet_rejected / user_blocked

SERVER (Node.js)
    ├─ Socket Handlers
    │  ├─ join_user → UserService.findOrCreateUser(name)
    │  ├─ join_market → MarketService.getState(symbol)
    │  ├─ place_bet → BetService.placeBet()
    │  └─ send_chat_message → ChatService.postMessage()
    │
    ├─ Market Service (Round Loop - cada 1s)
    │  ├─ openRound() → Genera nueva ronda
    │  ├─ onPriceTick() → Actualiza precio actual
    │  └─ settleRound() → Calcula ganadores + payouts
    │
    ├─ Streams
    │  ├─ Python Binance → Precios reales (WebSocket)
    │  └─ Fallback Generator → Si Binance ≤12s sin datos
    │
    └─ Redis Repository
       ├─ user:{name} → { id, name, balance, blocked }
       ├─ round:{symbol}:current → { id, startPrice, endPrice, result }
       ├─ round:{roundId}:{symbol}:bets → { userId: Bet }
       ├─ chat:{symbol}:messages → [ChatMessage]
       └─ leaderboard:{symbol} → Sorted Set por balance
```

## 🔌 API y Socket Events

### Socket.IO - Cliente → Servidor

```javascript
// Login
socket.emit("join_user", { name: "juan" });

// Entrar a mercado
socket.emit("join_market", { symbol: "ETHUSDT" });

// Apostar
socket.emit("place_bet", { symbol: "ETHUSDT", side: "up", amount: 50 });

// Chat
socket.emit("send_chat_message", { symbol: "ETHUSDT", text: "¡Vamos!" });
```

### Socket.IO - Servidor → Cliente

```javascript
// Usuario conectado
socket.on("user_joined", ({ user }) => {
  // { id: "juan", name: "juan", balance: 2000, blocked: false }
});

// Mercado conectado (estado inicial)
socket.on("market_joined", ({ symbol, user, round, chat, roomBoard }) => {
  // symbol: "ETHUSDT"
  // user: { id, name, balance, blocked }
  // round: { id, startPrice, endPrice, result, status }
  // chat: [{ id, userName, text, ts }]
  // roomBoard: [{ id, name, score }]
});

// Tick de precio (cada ~500ms)
socket.on("price_tick", ({ symbol, price }) => {
  // price: 3245.50
});

// Ronda iniciada
socket.on("round_started", ({ symbol, round, secondsLeft }) => {
  // round: { id, startPrice, endAt, lockAt }
  // secondsLeft: 20
});

// Timer cada segundo
socket.on("round_timer", ({ symbol, secondsLeft, lock }) => {
  // secondsLeft: 15, lock: false
});

// Apuesta bloqueada
socket.on("round_locked", ({ symbol }) => {
  // Los últimos 5 segundos
});

// Ronda terminada con payouts
socket.on("round_ended", ({ symbol, round, payouts }) => {
  // round: { id, startPrice, endPrice, result }
  // payouts: [{ userId, userName, betSide, amount, won, payout, balance, bonusMultiplier }]
});

// Leaderboard actualizado
socket.on("leaderboard_updated", ({ symbol, roomBoard }) => {
  // roomBoard: [{ id, name, score }]
});

// Mensaje de chat
socket.on("chat_message", ({ symbol, userName, text, ts }) => {
  // userName: "juan", text: "¡UP!"
});

// Acción de usuario (sistema)
socket.on("user_action", ({ symbol, message }) => {
  // message: "juan joined ETHUSDT"
});

// Apuesta aceptada
socket.on("bet_accepted", ({ balance, blocked }) => {
  // balance: 1950 (después de apostar 50)
});

// Apuesta rechazada
socket.on("bet_rejected", ({ message }) => {
  // message: "Round is not open"
});

// Usuario bloqueado (balance ≤ 0)
socket.on("user_blocked", ({ message, balance }) => {
  // message: "Has perdido por completo..."
  // balance: 0
});

// Error general
socket.on("error_message", ({ message }) => {
  // message: error description
});
```

## 💾 Almacenamiento de Datos

### Redis

```
user:{name}
  └─ { id, name, balance, blocked, createdAt }
  
username:{name} → {id}
  └─ Índice para búsquedas rápidas por nombre

round:{symbol}:current
  └─ { id, symbol, startPrice, endPrice, result, status, startAt, endAt, lockAt, locked }

round:{symbol}:history
  └─ [{ id, symbol, startPrice, endPrice, result, closedAt, bets }]

round:{roundId}:{symbol}:bets
  └─ { userId: { id, roundId, symbol, userId, userName, side, amount, timestamp } }

round:{roundId}:{symbol}:speed:{side}
  └─ Sorted Set para bonus de velocidad (timestamp → userId)

chat:{symbol}:messages
  └─ [{ id, symbol, userName, text, ts }]

leaderboard:{symbol}
  └─ Sorted Set { balance: "userId|userName" }
```

### Kafka (Auditoría)

```
market.prices.raw
  └─ { symbol, price, ts, source }

market.bets.events
  └─ { type, symbol, roundId, userId, userName, side, amount, balance, ts }

market.round.events
  └─ { type, symbol, roundId, startPrice, endPrice, result, ts }
```

## 📱 Diseño Responsive

```
Breakpoints y comportamiento:

Desktop (1200px+)
├─ Layout: 2 columnas (1.25fr 1fr)
├─ Izquierda: Precio, Timer, Round, Leaderboard
└─ Derecha: Chat, Panel de Apuestas

Tablet (800-1200px)
├─ Layout: 1 columna
├─ Todos los elementos apilados verticalmente
└─ Fuentes escaladas con clamp()

Mobile (480-800px)
├─ Layout: Ultra-compacto
├─ Gráfico reducido
├─ Chat y botones optimizados
└─ Precio y timer reducidos

Móvil pequeño (<480px)
├─ Mínimo espacio utilizado
├─ Fuentes mínimas legibles
├─ Botones tocables
└─ Todo funciona sin horizontal scroll
```

### Unidades Responsivas

```css
/* Tamaños que escalan automáticamente */
font-size: clamp(12px, 2vw, 16px);      /* Escala con viewport */
padding: clamp(8px, 2vw, 12px);         /* Espacios dinámicos */
gap: clamp(6px, 1.5vw, 10px);          /* Brechas fluidas */
max-height: clamp(120px, 25vh, 180px); /* Altura relativa */
```

## 🔄 Flujo Completo de una Ronda

```
1. INICIO (t=0)
   ├─ marketService.openRound(symbol)
   ├─ Round.startPrice = precio actual
   ├─ Emite: "round_started"
   └─ Estado: "open"

2. APUESTAS ABIERTAS (t=0-14s)
   ├─ Usuarios pueden apostar
   ├─ betService.placeBet() guarda bets
   ├─ Socket emite: "user_action"
   └─ Precio se actualiza: "price_tick"

3. TIMER AMARILLO (t=10-14s)
   ├─ Emite: "round_timer" con lock=false
   ├─ UI cambia timer a amarillo
   └─ Usuarios ven que quedan pocos segundos

4. BLOQUEO (t=15s exactamente)
   ├─ Emite: "round_locked"
   ├─ UI: Timer rojo, botones deshabilitados
   ├─ Mensaje: "Bet lock active (last 5s)"
   └─ No se aceptan más apuestas

5. TERMINO (t=20s)
   ├─ marketService.settleRound(symbol)
   ├─ Round.endPrice = precio final
   ├─ Round.result = determineResult(start, end)
   ├─ Calcula ganadores y payouts
   ├─ Actualiza balances en Redis
   ├─ Actualiza leaderboards
   ├─ Emite: "round_ended" con payouts
   ├─ Emite: "leaderboard_updated"
   └─ Espera 800ms y vuelve a paso 1

Después de 800ms → Nueva ronda automática
```

## 🎯 Cambios Recientes Implementados

### ✅ Nombre de Usuario Único
- Eliminamos UUID
- El nombre es el identificador único
- Sesiones persistentes por nombre
- Pre-llenado automático en login

### ✅ Gráfico en Tiempo Real
- Actualización cada 5 segundos
- 120 puntos almacenados (10 minutos)
- Colores dinámicos (verde/rojo/amarillo)
- Interpolación suave
- Posicionado entre Round Details y Leaderboard

### ✅ Diseño Completamente Responsive
- Funciona en ventanas pequeñas (50% pantalla)
- Móvil-friendly desde 320px
- Escalado fluido sin saltos
- Todos los elementos adaptables

### ✅ Chat Mejorado
- Sin contador de mensajes
- Auto-scroll inteligente
- Botón "Latest" cuando scrolleas arriba
- Envío por Enter o botón

## 🚀 Features Principales

✅ **Real-time Streaming**: Precios en vivo de Binance  
✅ **WebSocket**: Socket.IO para comunicación bidireccional  
✅ **Persistencia**: Usuarios y balance guardados en Redis  
✅ **Leaderboard**: Competencia en vivo por sala  
✅ **Chat**: Comunicación instantánea  
✅ **Gráfico**: Visualización de precios con Chart.js  
✅ **Responsive**: Funciona en cualquier dispositivo  
✅ **Kafka**: Auditoría de eventos (opcional)  
✅ **Fallback**: Generador de precios si Binance falla  
✅ **Auto-login**: Recuperación de sesión por nombre  

## 📊 Mercados Soportados

- **ETHUSDT** - Ethereum vs USDT
- **SOLUSDT** - Solana vs USDT
- **BNBUSDT** - Binance Coin vs USDT

## 🛠️ Desarrollo

### Iniciar con logs
```bash
npm start
# Ver salida en consola
```

### Estructura de carpetas
```
src/
├── config/          # Configuración
├── models/          # Modelos de datos
├── repositories/    # Acceso a datos (Redis)
├── services/        # Lógica de negocio
├── controllers/     # Endpoints HTTP
├── sockets/         # Handlers Socket.IO
├── streams/         # Integración Binance
└── public/          # Frontend (HTML, CSS, JS)
```

### Logs importantes
```
[app] running on http://localhost:3000
[markets] ETHUSDT, SOLUSDT, BNBUSDT
[stream] python-binance via python
[kafka] mirror producer connected
```

## 📝 Git Workflow

```bash
# Ver status
git status

# Cambios realizados
git add .

# Commit con mensaje descriptivo
git commit -m "Descripción clara del cambio"

# Subir cambios
git push origin main
```

## ⚠️ .gitignore Configurado

El repositorio ignora automáticamente:
- `node_modules/` - Dependencias
- `.env` - Variables de entorno (credenciales)
- `*.log` - Archivos de log
- `.DS_Store` / `Thumbs.db` - Archivos del sistema
- `README.md` - Documentación local

## 🤝 Soporte

Para problemas o preguntas:
1. Revisar la consola del navegador (F12)
2. Revisar la salida del servidor (terminal)
3. Verificar Redis y Kafka estén corriendo
4. Abrir un issue en GitHub

## 📄 Licencia

MIT

---

**Última actualización**: 28/04/2026  
**Versión**: 1.0.0
