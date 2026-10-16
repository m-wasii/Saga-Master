import { Router } from "express";
import { example, kafkaConsumerExample, kafkaProducerExample, throwException } from "../controller/example";
import { asyncHandler } from "../utils/async-handler";

const router = Router();

router.post("/post-api", asyncHandler(example));
router.get("/get-api", asyncHandler(example));
router.put("/put-api", asyncHandler(example));
router.post("/fail-api", asyncHandler(throwException));
router.get("/kafka-producer-example", asyncHandler(kafkaProducerExample));
router.get("/kafka-consumer-example", asyncHandler(kafkaConsumerExample));

export { router as exampleRouter };
