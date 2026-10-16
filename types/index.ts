import { API_TYPE, COMMUNICATION_TYPE } from "../utils/enum";

export interface QueryParameter {
  key: string;
  value: string;
}

export interface KafkaPayload {
  key: string;
  value: unknown;
}

export interface KafkaProducerConfig {
  topic: string;
  messages?: KafkaPayload[];
}

export interface KafkaConsumerConfig {
  topic: string;
  fromBeginning?: boolean;
  groupId: string;
}

export interface SagaKafkaSetupData {
  communicateType: COMMUNICATION_TYPE.KAFKA | string;
  clientId: string;
  brokers: string[];
  serviceName: string;
  sendResponseToAPI?: boolean;
  producer: KafkaProducerConfig;
  consumer: KafkaConsumerConfig;
  compensateProducer: KafkaProducerConfig;
  compensateConsumer: KafkaConsumerConfig;
  successResponse?: unknown;
  response?: unknown;
  isSuccess?: boolean;
  isRollbackSuccessful?: boolean;
  rollbackResponse?: unknown;
}

export interface SagaRestSetupData {
  communicateType: COMMUNICATION_TYPE.REST | string;
  apiType: API_TYPE | string;
  apiUrl: string;
  compensateApiUrl?: string;
  compensateApiType?: API_TYPE | string;
  compensatePathVariable?: string;
  compensateQueryParameter?: QueryParameter[];
  serviceName: string;
  sendResponseToAPI: boolean;
  triggerCompensate: boolean;
  response?: unknown;
  isSuccess?: boolean;
  isRollbackSuccessful?: boolean;
  rollbackResponse?: unknown;
}

export interface Setup {
  url: string;
  setup: SagaRestSetupData[] | SagaKafkaSetupData[];
}

export interface PriorStep {
  serviceName: string;
  response: unknown;
}

export interface HttpError extends Error {
  status?: number;
}
