import { Request, Response } from "express";
import { tempData } from "./setup";
import {
  addPathAndQueryToUrlFromResponse,
  extractToken,
  logging,
  mergePriorResponses,
  runKafkaOrchestration,
} from "../utils/functions";
import { API_TYPE, COMMUNICATION_TYPE, LOGGING_EVENT_TYPE } from "../utils/enum";
import { HttpClient } from "../utils/http-client";
import { HttpError, PriorStep, SagaKafkaSetupData, SagaRestSetupData } from "../types";

let inFlight = false;

const orchestrate = async (req: Request, res: Response) => {
  if (inFlight) {
    res.status(409).json({ message: "An orchestration is already in progress." });
    return;
  }
  inFlight = true;
  try {
    await executeOrchestration(req, res);
  } finally {
    inFlight = false;
  }
};

async function executeOrchestration(req: Request, res: Response) {
  const orchestrateData = loadOrchestration(req.params.url);
  logging(LOGGING_EVENT_TYPE.START);
  const token = extractToken(req);
  const bodyData = req.body;
  const protocol = `${orchestrateData[0].communicateType}`.toUpperCase();
  const mixed = orchestrateData.some((step) => `${step.communicateType}`.toUpperCase() !== protocol);
  if (mixed) {
    res.status(400).json({ message: "Each setup must use one protocol, REST or Kafka." });
    return;
  }

  if (protocol === COMMUNICATION_TYPE.KAFKA) {
    await runKafkaOrchestration(orchestrateData as SagaKafkaSetupData[], bodyData, res);
    return;
  }

  const sagaManagerData: SagaRestSetupData[] = [];
  const prior: PriorStep[] = [];
  try {
    for (let index = 0; index < orchestrateData.length; index += 1) {
      const loopData = orchestrateData[index] as SagaRestSetupData;
      logging(LOGGING_EVENT_TYPE.REST_LOOP_IN_PROCESS, loopData, index);
      const payload = mergePriorResponses(bodyData, prior, Boolean(loopData.sendResponseToAPI));
      await runRestOrchestration(loopData, token, payload, sagaManagerData, index);
      const recorded = sagaManagerData[sagaManagerData.length - 1];
      if (recorded?.isSuccess) {
        prior.push({ serviceName: loopData.serviceName, response: recorded.response });
      }
    }
    logging(LOGGING_EVENT_TYPE.REST_SUCCESSFUL);
    const responseMessage = sagaManagerData.find((step) => !step.isSuccess)
      ? "Not all transactions were successful but no rollbacks were made."
      : "All transactions were successful";
    res.json({ message: responseMessage, responses: sagaManagerData });
  } catch (err) {
    const failedService = err instanceof Error && "failedService" in err
      ? String((err as Error & { failedService?: string }).failedService)
      : "unknown";
    try {
      const rollbackResponses: SagaRestSetupData[] = [];
      logging(LOGGING_EVENT_TYPE.REST_LOOP_COMPENSATION_START, { sagaManagerData, err });
      for (let index = 0; index < sagaManagerData.length; index += 1) {
        await runRestCompensationOrchestration(sagaManagerData, token, bodyData, rollbackResponses, index);
      }
      logging(LOGGING_EVENT_TYPE.REST_SUCCESSFUL);
      res.json({
        message: `${failedService} service failed. Rollbacks of successful transactions completed.`,
        responses: rollbackResponses,
      });
    } catch (rollbackErr) {
      logging(LOGGING_EVENT_TYPE.REST_FAILED);
      res.status(500).json({
        message: "Received an error while commencing rollback.",
        error: rollbackErr instanceof Error ? rollbackErr.message : rollbackErr,
      });
    }
  }
}

function loadOrchestration(url: string): SagaRestSetupData[] | SagaKafkaSetupData[] {
  if (tempData.length <= 0) {
    throw httpError("You need to setup the orchestrate in order to trigger a transaction.", 404);
  }
  const orchestrateData = tempData.find((entry) => entry.url === url)?.setup ?? [];
  if (!orchestrateData.length) {
    throw httpError(`You need to setup the orchestrate for /${url} in order to trigger a transaction.`, 404);
  }
  return orchestrateData;
}

function httpError(message: string, status: number): HttpError {
  const error: HttpError = new Error(message);
  error.status = status;
  return error;
}

class StepFailure extends Error {
  failedService: string;

  constructor(serviceName: string) {
    super(`Exception caught while processing on service ${serviceName}`);
    this.failedService = serviceName;
  }
}

const runRestOrchestration = async (
  loopData: SagaRestSetupData,
  token: string | null,
  bodyData: unknown,
  sagaManagerData: SagaRestSetupData[],
  index: number
) => {
  try {
    const responseData = await callApi(loopData.apiType, loopData.apiUrl, token, bodyData);
    sagaManagerData.push({ ...loopData, response: responseData, isSuccess: true });
    logging(LOGGING_EVENT_TYPE.REST_LOOP_SUCCESSFUL, null, index);
  } catch (error) {
    if (!loopData.triggerCompensate) {
      sagaManagerData.push({ ...loopData, response: errorText(error), isSuccess: false });
      return;
    }
    throw new StepFailure(loopData.serviceName);
  }
};

async function callApi(apiType: string, url: string, token: string | null, bodyData: unknown) {
  const method = apiType.toUpperCase();
  if (method === API_TYPE.POST) return HttpClient.post(url, token, bodyData);
  if (method === API_TYPE.GET) return HttpClient.get(url, token);
  if (method === API_TYPE.PUT) return HttpClient.put(url, token, bodyData);
  if (method === API_TYPE.DELETE) return HttpClient.delete(url, token);
  throw new StepFailure(url);
}

const runRestCompensationOrchestration = async (
  sagaManagerData: SagaRestSetupData[],
  token: string | null,
  bodyData: unknown,
  rollbackResponses: SagaRestSetupData[],
  index: number
) => {
  const successService = sagaManagerData[sagaManagerData.length - 1 - index];
  if (!successService?.compensateApiUrl || !successService.compensateApiType) return;

  logging(LOGGING_EVENT_TYPE.REST_LOOP_COMPENSATION_IN_PROGRESS, successService);
  const method = successService.compensateApiType.toUpperCase();
  const url = method === API_TYPE.PUT || method === API_TYPE.DELETE
    ? addPathAndQueryToUrlFromResponse(successService)
    : successService.compensateApiUrl;
  const responseOfRollbackService = await callApi(method, url, token, bodyData);
  rollbackResponses.push({
    ...successService,
    isRollbackSuccessful: true,
    rollbackResponse: responseOfRollbackService,
  });
};

function errorText(err: unknown): string {
  if (err instanceof Error) return err.message;
  return "Request failed";
}

export { orchestrate };
