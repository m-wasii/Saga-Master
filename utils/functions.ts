import { Request, Response } from "express";
import { Kafka, Consumer } from "kafkajs";
import { PriorStep, SagaKafkaSetupData, SagaRestSetupData } from "../types";
import { LOGGING_EVENT_TYPE } from "./enum";

export function extractToken(req: Request): string | null {
  if (req.headers.authorization && req.headers.authorization.split(" ")[0] === "Bearer") {
    return req.headers.authorization.split(" ")[1];
  }
  return null;
}

export function addPathAndQueryToUrlFromResponse(loopData: SagaRestSetupData): string {
  if (!loopData.compensateApiUrl) return "";
  const originalUrl = loopData.compensateApiUrl.replace(/\/$/, "");
  const key = loopData.compensatePathVariable;
  if (!key) return originalUrl;

  const response = loopData.response;
  let pathValue: unknown;
  if (Array.isArray(response)) {
    const match = response.find((item) => item && typeof item === "object" && key in (item as Record<string, unknown>));
    pathValue = match && typeof match === "object" ? (match as Record<string, unknown>)[key] : undefined;
  } else if (response && typeof response === "object") {
    pathValue = (response as Record<string, unknown>)[key];
  }

  if (pathValue === undefined || pathValue === null || `${pathValue}`.length === 0) return originalUrl;
  return `${originalUrl}/${pathValue}`;
}

export function mergePriorResponses(body: unknown, prior: PriorStep[], sendResponseToAPI: boolean): unknown {
  if (!sendResponseToAPI) return body;
  const base: Record<string, unknown> =
    body && typeof body === "object" && !Array.isArray(body)
      ? { ...(body as Record<string, unknown>) }
      : { payload: body };
  base.priorResponses = prior.map((step) => ({
    serviceName: step.serviceName,
    response: step.response,
  }));
  for (const step of prior) {
    base[step.serviceName] = step.response;
  }
  return base;
}

export function resolveBrokers(brokers: string[]): string[] {
  if (process.env.KAFKA_BROKERS) {
    return process.env.KAFKA_BROKERS.split(",").map((broker) => broker.trim()).filter(Boolean);
  }
  return brokers;
}

function errorText(err: unknown): string {
  if (err instanceof Error) return err.message;
  return "Unknown error";
}

export const logging = (event: LOGGING_EVENT_TYPE, data?: unknown, loopCounter?: number) => {
  const details = (data ?? {}) as {
    serviceName?: string;
    err?: unknown;
    sagaManagerData?: unknown;
    response?: unknown;
  };

  if (event === LOGGING_EVENT_TYPE.SETUP) console.log("Setting up orchestrate data:", data);
  if (event === LOGGING_EVENT_TYPE.START) console.log(`Orchestration process started at ${new Date().toLocaleString()}`);
  if (event === LOGGING_EVENT_TYPE.REST_LOOP_IN_PROCESS && loopCounter !== undefined) {
    console.log("====================================================");
    console.log(`Orchestration in progress. Starting transaction on ${details.serviceName}. Step ${loopCounter + 1}.`);
    console.log("Setup step:", data);
  }
  if (event === LOGGING_EVENT_TYPE.REST_LOOP_SUCCESSFUL && loopCounter !== undefined) {
    console.log(`Transaction ${loopCounter + 1} succeeded`);
  }
  if (event === LOGGING_EVENT_TYPE.REST_SUCCESSFUL) {
    console.log(`Orchestration process succeeded and ended at ${new Date().toLocaleString()}`);
  }
  if (event === LOGGING_EVENT_TYPE.REST_LOOP_COMPENSATION_START) {
    console.log("Rollback started.", details.err);
    console.log("Successful steps:", details.sagaManagerData);
  }
  if (event === LOGGING_EVENT_TYPE.REST_LOOP_COMPENSATION_IN_PROGRESS) {
    console.log(`Commencing rollback on service ${details.serviceName}`);
    console.log("Rollback data:", details.response);
  }
  if (event === LOGGING_EVENT_TYPE.REST_FAILED) {
    console.log("Received an error while commencing rollback");
    console.log(`Orchestration process failed and ended at ${new Date().toLocaleString()}`);
  }
};

function connectKafka(clientId: string, brokers: string[]) {
  return new Kafka({
    clientId,
    brokers: resolveBrokers(brokers),
    connectionTimeout: 10000,
    requestTimeout: 30000,
  });
}

export async function kafkaProducer(clientId: string, brokers: string[], topic: string, payload: unknown) {
  const kafka = connectKafka(clientId, brokers);
  const producer = kafka.producer();
  try {
    await producer.connect();
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(payload) }],
    });
    console.log(`Message sent to ${topic}`);
  } catch (err) {
    console.error("Kafka producer failed:", err);
    throw new Error("An exception was encountered in the Kafka producer. Refer to the logs for details.");
  } finally {
    await producer.disconnect().catch(() => undefined);
  }
}

interface ReplyListener {
  ready: Promise<void>;
  reply: Promise<Record<string, unknown>>;
  stop: () => Promise<void>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

interface KafkaTransport {
  produce(clientId: string, brokers: string[], topic: string, payload: unknown): Promise<void>;
  listen(clientId: string, brokers: string[], topic: string, groupId: string): Promise<ReplyListener>;
}

const defaultTransport: KafkaTransport = {
  produce: (clientId, brokers, topic, payload) => kafkaProducer(clientId, brokers, topic, payload),
  listen: (clientId, brokers, topic, groupId) => listenForReply(clientId, brokers, topic, groupId),
};

let activeTransport: KafkaTransport = defaultTransport;

export function useKafkaTransport(transport?: KafkaTransport): void {
  activeTransport = transport ?? defaultTransport;
}

export async function listenForReply(
  clientId: string,
  brokers: string[],
  topic: string,
  groupId: string
): Promise<ReplyListener> {
  const kafka = connectKafka(clientId, brokers);
  const consumer: Consumer = kafka.consumer({
    groupId: `${groupId}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });

  let markReady: () => void = () => undefined;
  const ready = new Promise<void>((resolve) => {
    markReady = resolve;
  });

  let markReply: (value: Record<string, unknown>) => void = () => undefined;
  let failReply: (err: Error) => void = () => undefined;
  const reply = new Promise<Record<string, unknown>>((resolve, reject) => {
    markReply = resolve;
    failReply = reject;
  });

  let settled = false;
  consumer.on(consumer.events.GROUP_JOIN, () => {
    setTimeout(() => markReady(), 500);
  });
  consumer.on(consumer.events.CRASH, (event) => {
    if (settled) return;
    settled = true;
    failReply(event.payload.error);
  });

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (settled || !message.value) return;
      let parsed: unknown;
      try {
        parsed = JSON.parse(message.value.toString());
      } catch {
        return;
      }
      if (!isRecord(parsed) || typeof parsed.success !== "boolean") return;
      settled = true;
      console.log(`Reply received from ${topic}`, parsed);
      markReply(parsed);
    },
  });

  return {
    ready,
    reply,
    stop: async () => {
      await consumer.stop().catch(() => undefined);
      await consumer.disconnect().catch(() => undefined);
    },
  };
}

async function stopListeners(listeners: ReplyListener[]) {
  await Promise.all(listeners.map((listener) => listener.stop()));
}

export async function runKafkaOrchestration(
  steps: SagaKafkaSetupData[],
  bodyData: unknown,
  res: Response
): Promise<void> {
  const succeeded: SagaKafkaSetupData[] = [];
  const responses: SagaKafkaSetupData[] = [];
  const prior: PriorStep[] = [];

  try {
    for (let index = 0; index < steps.length; index += 1) {
      const step = steps[index];
      logging(LOGGING_EVENT_TYPE.REST_LOOP_IN_PROCESS, step, index);
      const payload = mergePriorResponses(bodyData, prior, Boolean(step.sendResponseToAPI));
      const listener = await activeTransport.listen(step.clientId, step.brokers, step.consumer.topic, step.consumer.groupId);
      await listener.ready;
      await activeTransport.produce(step.clientId, step.brokers, step.producer.topic, payload);
      const reply = await listener.reply;
      await listener.stop();

      if (reply.success !== true) {
        await compensateKafka(succeeded, bodyData, res, step.serviceName);
        return;
      }

      const recorded = { ...step, response: reply, isSuccess: true };
      succeeded.push(step);
      responses.push(recorded);
      prior.push({ serviceName: step.serviceName, response: reply });
      logging(LOGGING_EVENT_TYPE.REST_LOOP_SUCCESSFUL, step, index);
    }

    logging(LOGGING_EVENT_TYPE.REST_SUCCESSFUL);
    res.json({ message: "All transactions were successful", responses });
  } catch (err) {
    if (res.headersSent) return;
    logging(LOGGING_EVENT_TYPE.REST_FAILED);
    res.status(500).json({ message: errorText(err) });
  }
}

async function compensateKafka(
  succeeded: SagaKafkaSetupData[],
  bodyData: unknown,
  res: Response,
  failedService: string
) {
  const reversed = [...succeeded].reverse();
  logging(LOGGING_EVENT_TYPE.REST_LOOP_COMPENSATION_START, {
    err: `${failedService} reported success false`,
    sagaManagerData: reversed.map((step) => step.serviceName),
  });

  if (reversed.length === 0) {
    res.json({
      message: `${failedService} service failed. No successful transactions required rollback.`,
      responses: [],
    });
    return;
  }

  const listeners: ReplyListener[] = [];
  try {
    for (const step of reversed) {
      const listener = await activeTransport.listen(
        step.clientId,
        step.brokers,
        step.compensateConsumer.topic,
        step.compensateConsumer.groupId
      );
      await listener.ready;
      listeners.push(listener);
      console.log(`Attached compensation consumer on ${step.compensateConsumer.topic}`);
    }

    const rollbackResponses: SagaKafkaSetupData[] = [];
    await activeTransport.produce(
      reversed[0].clientId,
      reversed[0].brokers,
      reversed[0].compensateProducer.topic,
      mergePriorResponses(bodyData, [], Boolean(reversed[0].sendResponseToAPI))
    );

    for (let index = 0; index < reversed.length; index += 1) {
      const step = reversed[index];
      const reply = await listeners[index].reply;
      if (reply.success !== true) {
        logging(LOGGING_EVENT_TYPE.REST_FAILED);
        await stopListeners(listeners);
        res.status(500).json({
          message: "Received an error while commencing rollback.",
          error: reply,
        });
        return;
      }

      rollbackResponses.push({
        ...step,
        isSuccess: true,
        isRollbackSuccessful: true,
        rollbackResponse: reply,
      } as SagaKafkaSetupData);
      logging(LOGGING_EVENT_TYPE.REST_LOOP_COMPENSATION_IN_PROGRESS, {
        serviceName: step.serviceName,
        response: reply,
      });

      const next = reversed[index + 1];
      if (next) {
        await activeTransport.produce(
          next.clientId,
          next.brokers,
          next.compensateProducer.topic,
          mergePriorResponses(bodyData, [], Boolean(next.sendResponseToAPI))
        );
      }
    }

    await stopListeners(listeners);
    logging(LOGGING_EVENT_TYPE.REST_SUCCESSFUL);
    res.json({
      message: `${failedService} service failed. Rollbacks of successful transactions completed.`,
      responses: rollbackResponses,
    });
  } catch (err) {
    await stopListeners(listeners);
    if (res.headersSent) return;
    logging(LOGGING_EVENT_TYPE.REST_FAILED);
    res.status(500).json({
      message: "Received an error while commencing rollback.",
      error: errorText(err),
    });
  }
}
