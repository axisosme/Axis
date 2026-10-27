#!/usr/bin/env node

import { runInitCli } from "../init-cli.js";

import { runAxisCli } from "../axis-cli.js";
import { runServeCli } from "../serve-cli.js";
import { isLifecycleCommand, runLifecycleCli } from "../lifecycle/cli.js";

const argv = process.argv.slice(2);
if (argv[0] === "init") {
  void runInitCli(argv.slice(1)).then(code => { process.exitCode = code; });
}
else if (argv[0] === "serve") void runServeCli(argv.slice(1)).then(code => { process.exitCode = code; });
else if (isLifecycleCommand(argv)) void runLifecycleCli(argv).then((code) => { process.exitCode = code; });
else process.exitCode = runAxisCli(argv);
