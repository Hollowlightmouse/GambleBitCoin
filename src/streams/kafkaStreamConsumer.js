const { log, warn, error } = require("../config/logger");

class KafkaStreamConsumer {
  constructor({ kafkaMirror, topics, groupId, onMessage }) {
    this.kafkaMirror = kafkaMirror;
    this.topics = topics;
    this.groupId = groupId;
    this.onMessage = onMessage;
    this.consumer = null;
  }

  async start() {
    if (!this.kafkaMirror || !this.kafkaMirror.createConsumer) return;
    this.consumer = this.kafkaMirror.createConsumer(this.groupId);
    if (!this.consumer) return;

    try {
      await this.consumer.connect();
      for (const topic of this.topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      }
      log("kafka", `consumer ${this.groupId} subscribed to ${this.topics.join(", ")}`);

      await this.consumer.run({
        eachMessage: async ({ topic, message }) => {
          try {
            const raw = message.value ? message.value.toString() : "";
            const payload = raw ? JSON.parse(raw) : null;
            if (!payload) return;
            this.onMessage(topic, payload);
          } catch (err) {
            warn("kafka", `consumer parse error: ${err.message}`);
          }
        },
      });
    } catch (err) {
      error("kafka", `consumer ${this.groupId} failed: ${err.message}`);
    }
  }

  async stop() {
    if (!this.consumer) return;
    try {
      await this.consumer.disconnect();
    } catch (_err) {
      // ignore
    }
  }
}

module.exports = { KafkaStreamConsumer };
