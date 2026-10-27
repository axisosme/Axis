import { LinearGraphQlProvider } from "../dist/relay/linear/provider.js";

const accessToken = process.env.LINEAR_DEV_ACCESS_TOKEN;
const agentSessionId = process.env.LINEAR_DEV_AGENT_SESSION_ID;
if (!accessToken || !agentSessionId) {
  process.stderr.write("Live Linear smoke unavailable: LINEAR_DEV_ACCESS_TOKEN and LINEAR_DEV_AGENT_SESSION_ID are required.\n");
  process.exitCode = 2;
} else {
  const provider = new LinearGraphQlProvider();
  const viewer = await provider.resolveViewer(accessToken);
  const activities = await provider.loadSessionActivities(accessToken, agentSessionId);
  process.stdout.write(JSON.stringify({ status: "ok", appUserResolved: viewer.appUserId.length > 0, activityCount: activities.length }) + "\n");
}
