import type { WorkerWorkspaceConfiguration } from "../shared/router-worker.js";

export interface WorkerRuntimeConfig { readonly database: string; readonly token: string; readonly host: string; readonly port: number; readonly routerEndpoint: string; readonly routerToken: string; readonly workerId: string; readonly executorId: string; readonly nodeId: string; readonly workspaces: readonly WorkerWorkspaceConfiguration[]; readonly profileFallbackPolicy: "allow" | "deny"; readonly codexHome?: string; }
