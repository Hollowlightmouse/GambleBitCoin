const { Kafka } = require("kafkajs");
const { log, error } = require("../config/logger");

class KafkaMirror {
  constructor(brokers) {
    this.ready = false;
    this.enabled = Array.isArray(brokers) && brokers.length > 0;
    this.producer = null;
    this.kafka = null;

    if (!this.enabled) return;

    this.kafka = new Kafka({
      clientId: "market-mirror",
      brokers,
    });
    this.producer = this.kafka.producer();
  }

  async connect() {
    if (!this.enabled || !this.producer) return;
    try {
      await this.producer.connect();
      this.ready = true;
      log("kafka", "mirror producer connected");
    } catch (err) {
      this.ready = false;
      error("kafka", `mirror unavailable: ${err.message}`);
    }
  }

  async send(topic, payload) {
    if (!this.ready || !this.producer) return;
    try {
      await this.producer.send({
        topic,
        messages: [{ value: JSON.stringify(payload) }],
      });
    } catch (err) {
      error("kafka", `send failed on ${topic}: ${err.message}`);
    }
  }

  createConsumer(groupId) {
    if (!this.kafka || !this.enabled) return null;
    return this.kafka.consumer({ groupId });
  }
}

module.exports = { KafkaMirror };
