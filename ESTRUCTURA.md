# Estructura del proyecto

Guia breve y entendible del prototipo, su estructura y el flujo de datos.

## 1) Carpetas y archivos principales

- `app.js`: punto de entrada. Levanta Express + Socket.IO, conecta Kafka, inicia el loop de rondas y el stream de precios.
- `docker-compose.yml`: orquesta Kafka y Zookeeper para el entorno local.
- `spark/spark_processor.py`: procesamiento en tiempo real con Spark (ventanas, watermarks, alertas y reportes).

### `src/`

- `src/public/`: frontend del prototipo (UI del juego).
- `src/public/index.html`: estructura del sitio (incluye el contenedor de “Anomaly Alerts”).
- `src/public/client.js`: logica del cliente (Socket.IO, grafica, alertas, chat).
- `src/public/styles.css`: estilos visuales del frontend.

- `src/config/`: configuracion y constantes.
- `src/config/constants.js`: mercados soportados y reglas base.
- `src/config/logger.js`: logs estandarizados.

- `src/services/`: logica principal del negocio.
- `src/services/marketService.js`: administra rondas, precios y emite eventos a sockets.
- `src/services/betService.js`: valida apuestas y actualiza balances.
- `src/services/userService.js`: crea/recupera usuarios.
- `src/services/leaderboardService.js`: genera rankings.
- `src/services/chatService.js`: publica mensajes.
- `src/services/priceUtils.js`: calcula resultados (UP/DOWN/HOLD).

- `src/streams/`: integracion de streaming y Kafka.
- `src/streams/kafkaMirror.js`: productor Kafka del backend.
- `src/streams/kafkaStreamConsumer.js`: consumidor Kafka que alimenta el servidor.
- `src/streams/binance_kafka_producer.py`: productor Python que lee Binance WS y publica ticks en Kafka.
- `src/streams/binanceKafkaProducer.js`: wrapper Node que lanza el productor Python (flujo activo).

- `src/sockets/`: eventos Socket.IO.
- `src/sockets/registerSocketHandlers.js`: eventos de login, mercado, apuestas y chat.

- `src/repositories/`: acceso a datos.
- `src/repositories/RedisRepository.js`: persistencia en Redis (usuarios, rondas, chat, leaderboard).

## 2) Docker y contenedores

### Conexion

- `docker-compose.yml` levanta:
  - **Zookeeper**: coordina Kafka.
  - **Kafka Broker**: recibe los topics del stream.
  - **Kafka UI**: inspecciona topics y mensajes.

### Como se conectan los servicios

- **Productor Python** (binance_kafka_producer.py) -> Kafka (topic `market.prices.raw`).
- **Spark** consume Kafka y emite:
  - alertas a `market.alerts`.
  - reportes CSV en `data/`.
- **Backend Node** consume `market.prices.raw` y `market.alerts`.
- **Frontend** recibe todo por Socket.IO.

## 3) Topics de Kafka y que almacenan

- `market.prices.raw`
  - Contenido: ticks crudos `{ symbol, price, ts, source }`.
  - Produce: `src/streams/binance_kafka_producer.py`.
  - Consume: `spark/spark_processor.py` y `app.js` (via `KafkaStreamConsumer`).

- `market.bets.events`
  - Contenido: eventos de apuestas `{ type, symbol, roundId, userId, side, amount, ts }`.
  - Produce: `src/services/betService.js` (via `KafkaMirror`).
  - Consume: Spark (para agregados por ventana).

- `market.round.events`
  - Contenido: eventos de ronda `{ type, symbol, roundId, startPrice, endPrice, result, ts }`.
  - Produce: `src/services/marketService.js`.
  - Consume: opcional (auditoria).

- `market.alerts`
  - Contenido: alertas y reportes `{ alert_type, trend, avg_price, min_price, max_price, window_start, window_end }`.
  - Produce: `spark/spark_processor.py`.
  - Consume: `app.js` y la UI con evento `anomaly_alert`.

## 4) Flujo del sistema (resumen claro)

```
Binance WS -> Python Producer -> Kafka (market.prices.raw)
                     |                    |
                     |                    +--> Backend Node -> Socket.IO -> UI (price_tick)
                     |
                     +--> Spark -> ventanas + watermark
                               -> alertas/reportes -> Kafka (market.alerts)
                               -> CSV en data/

UI
 - grafica estable (intervalo fijo)
 - Anomaly Alerts (trend + promedio)
 - rondas de apuesta en tiempo real
```

## 5) Basicos que debes saber al exponer

- El sistema **transforma** datos: no solo mueve ticks, los convierte en agregados por ventana.
- Spark usa **watermarks** para manejar datos tardios y evitar re-procesar indefinidamente.
- El panel **Anomaly Alerts** muestra tendencia (UP/DOWN/HOLD) y promedio.
- Las rondas se abren y cierran automaticamente y emiten eventos al frontend.
