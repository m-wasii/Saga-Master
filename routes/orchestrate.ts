import { Router } from "express";
import { orchestrate } from "../controller/orchestrate";
import { deleteSetup, getSetup, setSetup, updateSetup } from "../controller/setup";
import { asyncHandler } from "../utils/async-handler";

const router = Router();

router.post("/trigger/:url", asyncHandler(orchestrate));
router.post("/setup", asyncHandler(setSetup));
router.put("/setup/:url", asyncHandler(updateSetup));
router.delete("/setup/:url", asyncHandler(deleteSetup));
router.get("/setup", asyncHandler(getSetup));

export { router as orchestrateRouter };
