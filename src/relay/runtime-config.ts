export interface RelayVersionedReference { readonly ref: string; readonly version: string; }
export interface RelayWorkerIdentity { readonly nodeId: string; readonly workerId: string; readonly executorKind: string; readonly runtimeProfile: RelayVersionedReference; }
/** Volatile physical Node transport configuration; its URL and credential are never Router state. */
export interface RelayNodeEndpoint { readonly nodeRef: RelayVersionedReference; readonly nodeId: string; readonly endpoint: string; readonly bearerToken: string; }
/** One logical Worker target multiplexed through an exact physical Node reference. */
export interface RelayNodeTarget { readonly endpointRef: RelayVersionedReference; readonly nodeRef: RelayVersionedReference; readonly identity: RelayWorkerIdentity; readonly sessionBindingExecutorId: string; }
export interface RelayRuntimeConfig { readonly routerDatabase: string; readonly routerHost: string; readonly routerPort: number; readonly nodeEndpoints: readonly RelayNodeEndpoint[]; readonly nodeTargets: readonly RelayNodeTarget[]; readonly defaultEndpointRef?: RelayVersionedReference; readonly defaultExcludedTeamIds: readonly string[]; readonly nativeSteer?: boolean; }
