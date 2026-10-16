const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.join(__dirname, "..");
const children = [];

test.after(() => {
  for (const child of children) child.kill();
});

test("REST checkout succeeds and payment failure rolls back through the thin apps", async () => {
  start(["demo/order/index.js"]);
  start(["demo/inventory/index.js"]);
  start(["demo/notification/index.js"]);
  const payment = start(["demo/payment/index.js"], { FAIL_PAYMENT: "false" });
  start(["dist/index.js"], { PORT: "8891" });

  await waitFor("http://127.0.0.1:8080/api/invoices");
  await waitFor("http://127.0.0.1:8000/api/products");
  await waitFor("http://127.0.0.1:8082/api/payments");
  await waitFor("http://127.0.0.1:8089/notifications");
  await waitFor("http://127.0.0.1:8891/api/orchestrate/setup");

  const setup = JSON.parse(fs.readFileSync(path.join(root, "fake-data", "ms-test-setup.json"), "utf8"));
  const setupResponse = await fetch("http://127.0.0.1:8891/api/orchestrate/setup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(setup),
  });
  assert.equal(setupResponse.status, 200);

  const happy = await trigger({ item: "book", amount: 42 });
  assert.equal(happy.message, "All transactions were successful");
  assert.equal(happy.responses.length, 4);
  assert.equal(happy.responses.every((step) => step.isSuccess), true);
  const inventory = happy.responses.find((step) => step.serviceName === "inventory");
  assert.equal(inventory.response.received.priorResponses[0].serviceName, "order");

  payment.kill();
  await waitUntilDown("http://127.0.0.1:8082/api/payments");
  start(["demo/payment/index.js"], { FAIL_PAYMENT: "true" });
  await waitFor("http://127.0.0.1:8082/api/payments");

  const failed = await trigger({ item: "book", amount: 42 });
  assert.match(failed.message, /payment service failed/i);
  assert.match(failed.message, /rollback/i);
  assert.equal(failed.responses[0].serviceName, "inventory");
  assert.equal(failed.responses[1].serviceName, "order");
  assert.equal(failed.responses[0].isRollbackSuccessful, true);
  assert.equal(failed.responses[1].isRollbackSuccessful, true);

  const orderAgain = await fetch(`http://127.0.0.1:8080/api/invoices/${failed.responses[1].response.id}`, { method: "DELETE" });
  const productAgain = await fetch(`http://127.0.0.1:8000/api/products/${failed.responses[0].response.id}`, { method: "DELETE" });
  assert.equal(orderAgain.status, 404);
  assert.equal(productAgain.status, 404);
});

test("Kafka success false starts compensation in reverse order", async () => {
  const { runKafkaOrchestration, useKafkaTransport } = require(path.join(root, "dist", "utils", "functions"));
  const setup = JSON.parse(fs.readFileSync(path.join(root, "fake-data", "kafka-setup.json"), "utf8"));
  const topics = setup.setup.flatMap((step) => [
    step.producer.topic,
    step.consumer.topic,
    step.compensateProducer.topic,
    step.compensateConsumer.topic,
  ]);
  assert.equal(new Set(topics).size, topics.length);

  try {
    const bus = createBus();
    attachParticipants(bus, setup.setup, false);
    useKafkaTransport(bus.transport);
    const response = mockResponse();
    await runKafkaOrchestration(setup.setup, { item: "book", amount: 42 }, response);

    assert.equal(response.statusCode, 200);
    assert.match(response.body.message, /payment service failed/i);
    assert.equal(response.body.responses[0].serviceName, "inventory");
    assert.equal(response.body.responses[1].serviceName, "order");
    assert.deepEqual(commandTopics(bus.events), ["saga.order.command", "saga.inventory.command", "saga.payment.command"]);

    const inventoryCommand = bus.events.find((event) => event.topic === "saga.inventory.command");
    assert.equal(inventoryCommand.payload.priorResponses[0].serviceName, "order");

    const compensationSubscribes = bus.events.filter(
      (event) => event.type === "subscribe" && event.topic.endsWith(".compensate.reply")
    );
    const compensationCommands = bus.events.filter(
      (event) => event.type === "produce" && event.topic.endsWith(".compensate")
    );
    assert.deepEqual(compensationSubscribes.map((event) => event.topic), [
      "saga.inventory.compensate.reply",
      "saga.order.compensate.reply",
    ]);
    assert.deepEqual(compensationCommands.map((event) => event.topic), [
      "saga.inventory.compensate",
      "saga.order.compensate",
    ]);
    assert.ok(bus.events.indexOf(compensationSubscribes.at(-1)) < bus.events.indexOf(compensationCommands[0]));

    const failing = createBus();
    attachParticipants(failing, setup.setup, true);
    useKafkaTransport(failing.transport);
    const failed = mockResponse();
    await runKafkaOrchestration(setup.setup, { item: "book", amount: 42 }, failed);
    assert.equal(failed.statusCode, 500);
    assert.match(failed.body.message, /rollback/i);
    assert.deepEqual(
      failing.events.filter((event) => event.type === "produce" && event.topic.endsWith(".compensate")).map((event) => event.topic),
      ["saga.inventory.compensate"]
    );
  } finally {
    useKafkaTransport();
  }
});

function start(args, env = {}) {
  const child = spawn(process.execPath, args, {
    cwd: root,
    env: { ...process.env, ...env },
    stdio: "inherit",
  });
  children.push(child);
  return child;
}

async function trigger(body) {
  const response = await fetch("http://127.0.0.1:8891/api/orchestrate/trigger/create-payment-intend", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  assert.equal(response.status, 200, JSON.stringify(payload));
  return payload;
}

async function waitFor(url) {
  const deadline = Date.now() + 15000;
  let last = "unreachable";
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.status < 500) return;
      last = `status ${response.status}`;
    } catch (error) {
      last = error instanceof Error ? error.message : "unreachable";
    }
    await delay(200);
  }
  throw new Error(`Timed out waiting for ${url}: ${last}`);
}

async function waitUntilDown(url) {
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    try {
      await fetch(url);
    } catch {
      return;
    }
    await delay(100);
  }
  throw new Error(`${url} did not stop`);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function mockResponse() {
  return {
    statusCode: 200,
    body: null,
    headersSent: false,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      this.headersSent = true;
      return this;
    },
  };
}

function createBus() {
  const handlers = new Map();
  const events = [];
  const transport = {
    async produce(_clientId, _brokers, topic, payload) {
      events.push({ type: "produce", topic, payload });
      for (const handler of [...(handlers.get(topic) || [])]) handler(payload);
    },
    async listen(_clientId, _brokers, topic, groupId) {
      events.push({ type: "subscribe", topic, groupId });
      let markReply = () => undefined;
      const reply = new Promise((resolve) => {
        markReply = resolve;
      });
      const handler = (payload) => {
        if (payload && typeof payload.success === "boolean") markReply(payload);
      };
      const list = handlers.get(topic) || [];
      list.push(handler);
      handlers.set(topic, list);
      return {
        ready: Promise.resolve(),
        reply,
        stop: async () => {
          handlers.set(topic, (handlers.get(topic) || []).filter((item) => item !== handler));
        },
      };
    },
  };
  return { transport, handlers, events };
}

function attachParticipants(bus, steps, failInventoryCompensation) {
  for (const step of steps) {
    const commands = bus.handlers.get(step.producer.topic) || [];
    commands.push(() => {
      const success = step.serviceName === "payment" ? false : true;
      void bus.transport.produce("participant", step.brokers, step.consumer.topic, { success });
    });
    bus.handlers.set(step.producer.topic, commands);

    const compensations = bus.handlers.get(step.compensateProducer.topic) || [];
    compensations.push(() => {
      const success = failInventoryCompensation && step.serviceName === "inventory" ? false : true;
      void bus.transport.produce("participant", step.brokers, step.compensateConsumer.topic, { success });
    });
    bus.handlers.set(step.compensateProducer.topic, compensations);
  }
}

function commandTopics(events) {
  return events.filter((event) => event.type === "produce" && event.topic.endsWith(".command")).map((event) => event.topic);
}
