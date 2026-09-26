// QA FIXTURE for VulnGraph cross-repo taint validation. Synthetic service, never deployed.
// Intentionally NO authentication on the consumer path (the trust-boundary case under test).
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: "postgres://payments:payments@postgres:5432/payments",
});

const kafka = new Kafka({ clientId: "payment-service", brokers: ["kafka:9092"] });
const consumer = kafka.consumer({ groupId: "payment-service" });

async function main() {
  await consumer.connect();
  // Consumes the shared "order-events" topic produced by order-service.
  await consumer.subscribe({ topic: "order-events", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      // Deliberately unsafe raw SQL write: tainted fields straight into the query string.
      await pool.query(
        "INSERT INTO payments (user_id, card, sku) VALUES ('" +
          order.userId + "', '" + order.card + "', '" + order.sku + "')"
      );
    },
  });
}

main().catch(console.error);
