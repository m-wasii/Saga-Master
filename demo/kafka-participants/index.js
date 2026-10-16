const path = require("path");
const { Kafka } = require("kafkajs");

const setup = require(path.join(__dirname, "..", "..", "fake-data", "kafka-setup.json"));
const brokers = (process.env.KAFKA_BROKERS || setup.setup[0].brokers.join(",")).split(",").map((broker) => broker.trim());
const failPayment = process.env.FAIL_PAYMENT === "true";

function topicsFor(step) {
  return [
    step.producer.topic,
    step.consumer.topic,
    step.compensateProducer.topic,
    step.compensateConsumer.topic,
  ];
}

async function joinConsumer(kafka, groupId, topic, handler) {
  const consumer = kafka.consumer({ groupId });
  const joined = new Promise((resolve) => {
    consumer.on(consumer.events.GROUP_JOIN, () => {
      setTimeout(resolve, 500);
    });
  });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      let body = {};
      try {
        body = JSON.parse(message.value?.toString() || "{}");
      } catch {
        body = {};
      }
      await handler(body);
    },
  });
  await joined;
}

async function connectAdmin(kafka) {
  const admin = kafka.admin();
  let lastError;
  for (let attempt = 1; attempt <= 30; attempt += 1) {
    try {
      await admin.connect();
      return admin;
    } catch (err) {
      lastError = err;
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
  throw lastError;
}

async function main() {
  const kafka = new Kafka({ clientId: "saga-participants", brokers });
  const admin = await connectAdmin(kafka);
  const topicNames = setup.setup.flatMap((step) => topicsFor(step));
  await admin.createTopics({
    waitForLeaders: true,
    topics: topicNames.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 1 })),
  });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  async function reply(topic, payload) {
    await producer.send({ topic, messages: [{ value: JSON.stringify(payload) }] });
    console.log(`Replied on ${topic}`, payload);
  }

  for (const step of setup.setup) {
    await joinConsumer(kafka, `participant-${step.serviceName}-command`, step.producer.topic, async () => {
      const success = step.serviceName === "payment" && failPayment ? false : true;
      await reply(step.consumer.topic, { success });
    });
    await joinConsumer(kafka, `participant-${step.serviceName}-compensate`, step.compensateProducer.topic, async () => {
      await reply(step.compensateConsumer.topic, { success: true });
    });
  }

  console.log("Kafka participants are listening");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
