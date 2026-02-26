import init, { bootstrap_worker } from "./pkg/gibblox_pipeline_pump_web.js";

async function run() {
  await init();
  bootstrap_worker();
}

run().catch((error) => {
  const message = error && error.stack ? error.stack : String(error);
  self.postMessage({ cmd: "error", error: `worker bootstrap failed: ${message}` });
});
