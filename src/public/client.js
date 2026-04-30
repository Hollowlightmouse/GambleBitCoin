const socket = io();

const state = {
  user: null,
  symbol: null,
  blocked: false,
  chatAutoScroll: true,
  priceChart: null,
  chartData: [],
  lastChartUpdate: 0,
  lastPrice: null,
  alertItems: [],
};

const $ = (id) => document.getElementById(id);

function initChart() {
  const ctx = $("priceChart");
  if (!ctx) return;

  state.priceChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: [],
      datasets: [
        {
          label: "Price",
          data: [],
          borderColor: "#00ff00",
          backgroundColor: "rgba(0, 255, 0, 0.1)",
          borderWidth: 3,
          fill: true,
          pointRadius: 0,
          pointHoverRadius: 0,
          tension: 0.4,
          spanGaps: true,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      animation: {
        duration: 0,
      },
      plugins: {
        legend: { display: false },
        filler: { propagate: true },
      },
      scales: {
        y: {
          beginAtZero: false,
          grid: { color: "rgba(255, 255, 255, 0.1)" },
          ticks: { color: "rgba(255, 255, 255, 0.7)" },
        },
        x: {
          grid: { display: false },
          ticks: { color: "rgba(255, 255, 255, 0.7)" },
        },
      },
    },
  });
}

function updateChart(price) {
  if (!state.priceChart) return;

  const now = Date.now();
  
  // Solo agregar punto cada 5 segundos (5000ms)
  if (now - state.lastChartUpdate < 5000) {
    state.lastPrice = price; // Actualiza el precio pero no renderiza
    return;
  }

  // 5 segundos pasaron, agregar punto
  state.lastChartUpdate = now;
  state.lastPrice = price;

  const timeLabel = new Date().toLocaleTimeString();

  state.chartData.push(price);
  
  // Mantener últimos 120 puntos (120 * 5s = 10 minutos)
  if (state.chartData.length > 120) {
    state.chartData.shift();
  }

  state.priceChart.data.labels.push(timeLabel);
  if (state.priceChart.data.labels.length > 120) {
    state.priceChart.data.labels.shift();
  }

  state.priceChart.data.datasets[0].data = [...state.chartData];

  // Color dinámico: verde si sube, rojo si baja
  const current = state.chartData[state.chartData.length - 1];
  const previous = state.chartData[state.chartData.length - 2] || current;

  if (current > previous) {
    state.priceChart.data.datasets[0].borderColor = "#00ff00";
    state.priceChart.data.datasets[0].backgroundColor = "rgba(0, 255, 0, 0.1)";
  } else if (current < previous) {
    state.priceChart.data.datasets[0].borderColor = "#ff0000";
    state.priceChart.data.datasets[0].backgroundColor = "rgba(255, 0, 0, 0.1)";
  } else {
    state.priceChart.data.datasets[0].borderColor = "#ffff00";
    state.priceChart.data.datasets[0].backgroundColor = "rgba(255, 255, 0, 0.1)";
  }

  state.priceChart.update("none");
}

function setLoginMsg(msg) {
  $("loginMsg").textContent = msg || "";
}

function setBetStatus(msg) {
  $("betStatus").textContent = msg || "";
}

function addChatLine(text) {
  const box = $("chatBox");
  const row = document.createElement("div");
  row.className = "line";
  row.innerHTML = text;
  box.appendChild(row);

  if (state.chatAutoScroll) {
    requestAnimationFrame(() => {
      box.scrollTop = box.scrollHeight;
    });
  }
}

function addAlertItem(alert) {
  const box = $("alertBox");
  if (!box) return;

  const createdAt = alert.created_at || alert.createdAt || new Date().toISOString();
  const label = alert.alert_type || alert.type || "ALERT";
  const symbol = alert.symbol || "--";
  const minPrice = alert.min_price !== undefined ? Number(alert.min_price).toFixed(6) : "--";
  const maxPrice = alert.max_price !== undefined ? Number(alert.max_price).toFixed(6) : "--";

  const row = document.createElement("div");
  row.className = "alert-item";
  row.innerHTML = `<strong>${label}</strong> ${symbol} | min ${minPrice} / max ${maxPrice} <span class="muted">${createdAt}</span>`;
  box.prepend(row);

  state.alertItems.push(row);
  if (state.alertItems.length > 50) {
    const last = state.alertItems.shift();
    if (last && last.remove) last.remove();
  }
}

function updateChatStickiness() {
  const box = $("chatBox");
  const distanceToBottom = box.scrollHeight - box.scrollTop - box.clientHeight;
  state.chatAutoScroll = distanceToBottom < 20;
  $("chatLatestBtn").classList.toggle("hidden", state.chatAutoScroll);
}

function renderBoard(el, rows) {
  if (!rows || !rows.length) {
    el.innerHTML = '<div class="muted">No data yet</div>';
    return;
  }

  el.innerHTML = rows
    .map((row, index) => {
      const score = Number(row.score || 0).toFixed(2);
      return `<div class="line">#${index + 1} ${(row.name || row.id)} - ${score}</div>`;
    })
    .join("");
}

function updateTimer(secondsLeft) {
  const timer = $("timerLabel");
  timer.textContent = `${secondsLeft || 0}s`;
  timer.classList.remove("warn", "danger");

  if (secondsLeft <= 5) {
    timer.classList.add("danger");
  } else if (secondsLeft <= 10) {
    timer.classList.add("warn");
  }
}

function updateBalance(value) {
  if (!state.user) return;
  state.user.balance = Number(value);
  $("balanceLabel").textContent = Number(value).toFixed(2);
}

function lockUIBecauseLost(msg) {
  state.blocked = true;
  setBetStatus(msg || "Has perdido por completo. No puedes seguir apostando.");
  $("betUp").disabled = true;
  $("betDown").disabled = true;
  $("betHold").disabled = true;
  $("betAmount").disabled = true;
}

function place(side) {
  if (state.blocked) return;
  if (!state.symbol) return;

  const amount = Number($("betAmount").value);
  socket.emit("place_bet", { symbol: state.symbol, side, amount });
}

$("joinBtn").onclick = () => {
  const name = $("nameInput").value.trim();
  const symbol = $("symbolSelect").value;

  if (!name) {
    setLoginMsg("Please enter username");
    return;
  }

  state.symbol = symbol;
  socket.emit("join_user", { name });
};

$("chatBtn").onclick = () => {
  const text = $("chatInput").value.trim();
  if (!text || !state.symbol) return;

  const input = $("chatInput");
  input.value = "";
  input.focus();

  socket.emit("send_chat_message", { symbol: state.symbol, text });
};

$("chatInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    $("chatBtn").click();
  }
});

$("chatLatestBtn").onclick = () => {
  const box = $("chatBox");
  box.scrollTop = box.scrollHeight;
  state.chatAutoScroll = true;
  $("chatLatestBtn").classList.add("hidden");
};

$("chatBox").addEventListener("scroll", updateChatStickiness);

$("betUp").onclick = () => place("up");
$("betDown").onclick = () => place("down");
$("betHold").onclick = () => place("hold");

socket.on("user_joined", ({ user }) => {
  state.user = user;
  localStorage.setItem("pm_user_name", user.name);
  socket.emit("join_market", { symbol: state.symbol });
});

socket.on("market_joined", (payload) => {
  $("loginView").classList.add("hidden");
  $("gameView").classList.remove("hidden");

  state.symbol = payload.symbol;
  state.user = payload.user;
  state.blocked = Boolean(payload.user.blocked);

  $("marketLabel").textContent = payload.symbol;
  $("userLabel").textContent = payload.user.name;
  updateBalance(payload.user.balance);

  if (payload.latestPrice) {
    $("priceLabel").textContent = Number(payload.latestPrice).toFixed(6);
    state.chartData = [Number(payload.latestPrice)];
  }

  if (payload.round) {
    $("startPriceLabel").textContent = Number(payload.round.startPrice).toFixed(6);
  }

  $("chatBox").innerHTML = "";
  $("chatLatestBtn").classList.add("hidden");
  (payload.chat || []).forEach((message) => {
    addChatLine(`<b>${message.userName}</b>: ${message.text}`);
  });

  const chatBox = $("chatBox");
  chatBox.scrollTop = chatBox.scrollHeight;
  state.chatAutoScroll = true;

  renderBoard($("boardRoom"), payload.roomBoard || []);

  initChart();

  const alertBox = $("alertBox");
  if (alertBox) alertBox.innerHTML = "";

  if (state.blocked) {
    lockUIBecauseLost("Has perdido por completo. No puedes seguir apostando.");
  }
});

socket.on("price_tick", ({ symbol, price }) => {
  if (symbol !== state.symbol) return;
  $("priceLabel").textContent = Number(price).toFixed(6);
  updateChart(Number(price));
});

socket.on("round_started", ({ symbol, round, secondsLeft }) => {
  if (symbol !== state.symbol) return;
  $("startPriceLabel").textContent = Number(round.startPrice).toFixed(6);
  $("endPriceLabel").textContent = "--";
  $("resultLabel").textContent = "--";
  $("roundStatus").textContent = "Round open: place your bet";
  setBetStatus("");
  updateTimer(secondsLeft);
});

socket.on("round_timer", ({ symbol, secondsLeft, lock }) => {
  if (symbol !== state.symbol) return;
  updateTimer(secondsLeft);
  if (lock) {
    $("roundStatus").textContent = "Bet lock active (last 5s)";
  }
});

socket.on("round_locked", ({ symbol }) => {
  if (symbol !== state.symbol) return;
  $("roundStatus").textContent = "Bet lock active (last 5s)";
});

socket.on("round_ended", ({ symbol, round, payouts }) => {
  if (symbol !== state.symbol) return;

  $("endPriceLabel").textContent = Number(round.endPrice).toFixed(6);
  $("resultLabel").textContent = round.result.toUpperCase();
  $("roundStatus").textContent = "Round ended. New round soon...";

  if (!state.user) return;

  const me = (payouts || []).find((row) => row.userId === state.user.id);
  if (me) {
    updateBalance(me.balance);
    if (me.won) {
      const bonusText = me.bonusMultiplier > 1 ? ` (speed bonus x${me.bonusMultiplier})` : "";
      setBetStatus(`You won! payout=${me.payout.toFixed(2)}${bonusText}`);
    } else {
      setBetStatus("You lost this round.");
    }

    if (me.balance <= 0) {
      lockUIBecauseLost("Has perdido por completo. No puedes seguir apostando.");
    }
  } else {
    setBetStatus("You did not participate in this round.");
  }
});

socket.on("leaderboard_updated", ({ symbol, roomBoard }) => {
  if (symbol !== state.symbol) return;
  renderBoard($("boardRoom"), roomBoard || []);
});

socket.on("chat_message", (message) => {
  if (message.symbol !== state.symbol) return;
  addChatLine(`<b>${message.userName}</b>: ${message.text}`);
});

socket.on("anomaly_alert", (payload) => {
  addAlertItem(payload);
});

socket.on("user_action", ({ symbol, message }) => {
  if (symbol !== state.symbol) return;
  addChatLine(`<span class="muted">${message}</span>`);
});

socket.on("bet_accepted", ({ balance, blocked }) => {
  updateBalance(balance);
  setBetStatus("Bet accepted");
  if (blocked) {
    lockUIBecauseLost("Has perdido por completo. No puedes seguir apostando.");
  }
});

socket.on("bet_rejected", ({ message }) => {
  setBetStatus(message || "Bet rejected");
});

socket.on("user_blocked", ({ message, balance }) => {
  updateBalance(balance || 0);
  lockUIBecauseLost(message || "Has perdido por completo. No puedes seguir apostando.");
});

socket.on("error_message", ({ message }) => {
  setLoginMsg(message || "Error");
  setBetStatus(message || "Error");
});

// Pre-llenar nombre si existe en localStorage
window.addEventListener("load", () => {
  const savedName = localStorage.getItem("pm_user_name");
  if (savedName) {
    $("nameInput").value = savedName;
  }
});
