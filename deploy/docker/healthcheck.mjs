import { loadAxisConfiguration } from "../../dist/configuration.js";

try {
  const config = loadAxisConfiguration(process.argv[2] ?? "/etc/axis/axis.toml");
  const response = await fetch(`http://127.0.0.1:${config.relay.port}/health/ready`, {
    redirect: "error",
    signal: AbortSignal.timeout(6_000),
  });
  await response.body?.cancel();
  process.exitCode = response.status === 200 ? 0 : 1;
} catch {
  process.exitCode = 1;
}
