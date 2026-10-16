import { Router } from "express";
import { compensate } from "../controller/compensate";
import { asyncHandler } from "../utils/async-handler";

const router = Router();

router.post("/trigger", asyncHandler(compensate));

export { router as compensateRouter };
