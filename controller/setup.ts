import { NextFunction, Request, Response } from "express";
import { SagaKafkaSetupData, SagaRestSetupData, Setup } from "../types";
import { logging } from "../utils/functions";
import { LOGGING_EVENT_TYPE } from "../utils/enum";

let tempData: Setup[] = [];

const exampleObject = [
  {
    communicateType: "REST",
    apiType: "POST",
    apiUrl: "http://localhost:8888/api/examples/post-api",
    compensateApiUrl: "http://localhost:8888/api/compensate/trigger",
    compensateApiType: "POST",
    serviceName: "order",
    sendResponseToAPI: false,
    triggerCompensate: true,
  },
];

const REST_KEYS = ["communicateType", "apiType", "apiUrl", "serviceName", "sendResponseToAPI", "triggerCompensate"];
const KAFKA_KEYS = ["communicateType", "clientId", "brokers", "serviceName", "sendResponseToAPI", "producer", "consumer", "compensateProducer", "compensateConsumer"];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function missingKeys(step: Record<string, unknown>, index: number): string[] {
  const protocol = `${step.communicateType ?? ""}`.toUpperCase();
  const required = protocol === "KAFKA" ? KAFKA_KEYS : REST_KEYS;
  const missing = required.filter((key) => step[key] === undefined || step[key] === null || step[key] === "");

  if (protocol === "REST" && step.triggerCompensate === true) {
    for (const key of ["compensateApiUrl", "compensateApiType"]) {
      if (!step[key]) missing.push(key);
    }
  }

  if (protocol === "KAFKA") {
    if (!Array.isArray(step.brokers) || step.brokers.length === 0) missing.push("brokers");
    for (const [name, fields] of [
      ["producer", ["topic"]],
      ["consumer", ["topic", "groupId"]],
      ["compensateProducer", ["topic"]],
      ["compensateConsumer", ["topic", "groupId"]],
    ] as const) {
      const value = step[name];
      if (!isRecord(value)) continue;
      for (const field of fields) {
        if (!value[field]) missing.push(`${name}.${field}`);
      }
    }
  }

  return missing.map((key) => `setup[${index}].${key}`);
}

function normalizeSetup(body: unknown): { url: string; setup: SagaRestSetupData[] | SagaKafkaSetupData[] } | { error: string } {
  if (!isRecord(body) || typeof body.url !== "string" || !body.url.trim()) {
    return { error: "url is required." };
  }
  if (!Array.isArray(body.setup) || body.setup.length === 0) {
    return { error: "setup must be a non-empty array." };
  }

  const missing: string[] = [];
  const setup = body.setup.map((step, index) => {
    if (!isRecord(step)) {
      missing.push(`setup[${index}]`);
      return step;
    }
    missing.push(...missingKeys(step, index));
    return {
      ...step,
      isSuccess: false,
      response: {},
      triggerCompensate: step.triggerCompensate ?? true,
    };
  });

  if (missing.length) {
    return { error: `Missing required setup keys: ${missing.join(", ")}` };
  }

  return { url: body.url, setup: setup as SagaRestSetupData[] | SagaKafkaSetupData[] };
}

const setSetup = async (req: Request, res: Response, _next: NextFunction) => {
  const normalized = normalizeSetup(req.body);
  if ("error" in normalized) {
    return res.status(400).json({ message: normalized.error, example: exampleObject });
  }

  tempData.unshift({ url: normalized.url, setup: normalized.setup });
  logging(LOGGING_EVENT_TYPE.SETUP, req.body);
  return res.json({ message: "Setup done successfully!", response: tempData });
};

const getSetup = async (_req: Request, res: Response, _next: NextFunction) => {
  return res.json(tempData);
};

const updateSetup = async (req: Request, res: Response, _next: NextFunction) => {
  const { url } = req.params;
  const indexOfSetupToBeUpdated = tempData.findIndex((entry) => entry.url === url);
  if (indexOfSetupToBeUpdated === -1) return res.status(404).json({ message: "The url provided doesn't exist in the system" });

  const normalized = normalizeSetup({ ...req.body, url });
  if ("error" in normalized) return res.status(400).json({ message: normalized.error, example: exampleObject });

  tempData[indexOfSetupToBeUpdated].setup = normalized.setup;
  return res.status(200).json({
    message: `/${url} setup is updated successfully. Below is the updated setup.`,
    setup: tempData,
  });
};

const deleteSetup = async (req: Request, res: Response, _next: NextFunction) => {
  const { url } = req.params;
  const indexOfDeleteSetup = tempData.findIndex((entry) => entry.url === url);
  if (indexOfDeleteSetup === -1) return res.status(404).json({ message: "The url provided doesn't exist in the system" });
  tempData.splice(indexOfDeleteSetup, 1);
  return res.status(202).json({
    message: `/${url} setup is deleted successfully. Below is the updated setup.`,
    setup: tempData,
  });
};

export { tempData, setSetup, getSetup, updateSetup, deleteSetup };
