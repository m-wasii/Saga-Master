import { NextFunction, Request, Response } from "express";
import { Kafka } from "kafkajs";

const example = async (req: Request, res: Response, _next: NextFunction) => {
  console.log("Example endpoint received:", req.body);
  return res.json({ message: "It works", received: req.body });
};

const throwException = async (_req: Request, _res: Response, next: NextFunction) => {
  next(new Error("This is a test error exception for saga orchestration."));
};

const kafkaProducerExample = async (_req: Request, res: Response) => {
  const kafka = new Kafka({
    clientId: "saga-master-example",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  });

  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: "saga-events",
    messages: [
      {
        key: "create-order",
        value: JSON.stringify({ id: 1, name: "order" }),
      },
    ],
  });
  await producer.disconnect();
  return res.json({ message: "It works" });
};

const kafkaConsumerExample = async (_req: Request, res: Response) => {
  const kafka = new Kafka({
    clientId: "saga-master-example",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  });
  const consumer = kafka.consumer({ groupId: "saga-master-example" });
  await consumer.connect();
  await consumer.subscribe({ topic: "saga-events", fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("Example consumer received:", message.value?.toString());
      await consumer.disconnect();
    },
  });
  return res.json({ message: "It works" });
};

export { example, throwException, kafkaProducerExample, kafkaConsumerExample };
