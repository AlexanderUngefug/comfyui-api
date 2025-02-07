import { createClient } from "redis";
import config from "./config";
import {
  isRedisPromptRequest,
  RedisPromptRequestSchema,
  WorkflowTree,
  Workflow,
  isWorkflow,
  ComfyPrompt,
} from "./types";
import workflows from "./workflows";
import {
  connectToComfyUIWebsocketStream,
  runPromptAndGetOutputs,
  validateAndPreProcessPrompt,
} from "./comfy";
import pino from "pino";
import { WebSocket } from "ws";
import { convertImageBuffer, getConfiguredWebhookHandlers } from "./utils";

function workflowExists(key: string): Workflow | null {
  const workflowPath = key.split("/");

  let current: Workflow | WorkflowTree = workflows;
  for (const path of workflowPath) {
    if (!current.hasOwnProperty(path)) {
      return null;
    }

    if (isWorkflow(current[path])) {
      return current[path];
    }

    current = current[path];
  }

  return null;
}

const log = pino({ level: config.logLevel });
let comfyWebsocketClient: WebSocket | null = null;
let queueDepth = 0;

export async function start() {
  const redisClient = await createClient({ url: config.redisURL }).connect();
  const handlers = getConfiguredWebhookHandlers(log);
  if (handlers.onStatus) {
    const originalHandler = handlers.onStatus;
    handlers.onStatus = (msg) => {
      queueDepth = msg.data.status.exec_info.queue_remaining;
      log.debug(`Queue depth: ${queueDepth}`);
      originalHandler(msg);
    };
  } else {
    handlers.onStatus = (msg) => {
      queueDepth = msg.data.status.exec_info.queue_remaining;
      log.debug(`Queue depth: ${queueDepth}`);
    };
  }
  comfyWebsocketClient = await connectToComfyUIWebsocketStream(
    handlers,
    log,
    true
  );

  redisClient.on("error", (err) => {
    console.error("Redis error", err);
  });

  while (true) {
    const message = await redisClient.BLPOP(config.redisQueueName, 0);
    if (!message) {
      continue;
    }

    const msg = JSON.parse(message.key);

    if (!msg.id) {
      console.error("Invalid message, no ID", message);
      continue;
    }

    const id = msg.id as string;
    const responseChannel = `${id}:response`;
    const progressChannel = `${id}:progress`;

    redisClient.PUBLISH(
      progressChannel,
      JSON.stringify({ type: "job.leased", ...config.systemMetaData })
    );

    if (!msg.route) {
      redisClient.PUBLISH(
        responseChannel,
        JSON.stringify({ code: 400, error: "No route specified" })
      );
      redisClient.PUBLISH(
        progressChannel,
        JSON.stringify({ type: "job.failed", ...config.systemMetaData })
      );
      continue;
    }

    if (!msg.body) {
      redisClient.PUBLISH(
        responseChannel,
        JSON.stringify({ code: 400, error: "No body specified" })
      );
      redisClient.PUBLISH(
        progressChannel,
        JSON.stringify({ type: "job.failed", ...config.systemMetaData })
      );
      continue;
    }

    const route = msg.route as string;
    const body = msg.body as any;

    let prompt: ComfyPrompt;

    if (route === "prompt") {
      if (!isRedisPromptRequest(body)) {
        console.error("Invalid message, invalid body", message);
        redisClient.PUBLISH(
          responseChannel,
          JSON.stringify({
            error: "Invalid body",
            issues: RedisPromptRequestSchema.safeParse(body).error,
          })
        );
        redisClient.PUBLISH(
          progressChannel,
          JSON.stringify({ type: "job.failed", ...config.systemMetaData })
        );
        continue;
      }
      prompt = body.prompt;
    } else if (route.startsWith("workflow/")) {
      const workflowKey = route.replace("workflow/", "");
      const workflow = workflowExists(workflowKey);

      if (!workflow) {
        console.error("Invalid message, invalid workflow route", message);
        redisClient.PUBLISH(
          responseChannel,
          JSON.stringify({ code: 404, error: "Workflow not found" })
        );
        redisClient.PUBLISH(
          progressChannel,
          JSON.stringify({ type: "job.failed", ...config.systemMetaData })
        );
        continue;
      }

      const parsed = workflow.RequestSchema.safeParse(body.input);
      if (!parsed.success) {
        console.error("Invalid message, invalid body.input", message);
        redisClient.PUBLISH(
          responseChannel,
          JSON.stringify({
            code: 400,
            error: "Invalid body",
            issues: parsed.error,
          })
        );
        redisClient.PUBLISH(
          progressChannel,
          JSON.stringify({ type: "job.failed", ...config.systemMetaData })
        );
        continue;
      }

      const input = parsed.data;
      prompt = await workflow.generateWorkflow(input);
    } else {
      console.error("Invalid message, invalid route", message);
      redisClient.PUBLISH(
        responseChannel,
        JSON.stringify({ code: 404, error: "Route not found" })
      );
      redisClient.PUBLISH(
        progressChannel,
        JSON.stringify({ type: "job.failed", ...config.systemMetaData })
      );
      continue;
    }

    try {
      prompt = await validateAndPreProcessPrompt(id, prompt, log);
    } catch (e: any) {
      console.error("Error validating prompt", e);
      redisClient.PUBLISH(
        responseChannel,
        JSON.stringify({ code: e.code, error: e.message, location: e.location })
      );
      redisClient.PUBLISH(
        progressChannel,
        JSON.stringify({ type: "job.failed", ...config.systemMetaData })
      );
      continue;
    }

    try {
      const outputs = await runPromptAndGetOutputs(id, prompt, log);
      if (body.convert_output) {
        for (const filename in outputs) {
          outputs[filename] = await convertImageBuffer(
            outputs[filename],
            body.convert_output
          );
        }
      }

      const base64Outputs: Record<string, string> = {};
      for (const filename in outputs) {
        base64Outputs[filename] = outputs[filename].toString("base64");
      }

      redisClient.PUBLISH(
        responseChannel,
        JSON.stringify({ code: 200, outputs: base64Outputs })
      );
      redisClient.PUBLISH(
        progressChannel,
        JSON.stringify({ type: "job.completed", ...config.systemMetaData })
      );
    } catch (e: any) {
      console.error("Error running prompt", e);
      redisClient.PUBLISH(
        responseChannel,
        JSON.stringify({ code: 500, error: e.message })
      );
      redisClient.PUBLISH(
        progressChannel,
        JSON.stringify({ type: "job.failed", ...config.systemMetaData })
      );
      continue;
    }
  }
}
