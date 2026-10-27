/**
 * One compiled registry per contract version serves the package validators. Schemas are immutable,
 * validators do not mutate registries, and validation errors are read synchronously before any await.
 * Failed construction is not cached. Public registry factories return a fresh registry on every call.
 */
export function memoizedRegistry<T>(create: () => Promise<T>): () => Promise<T> {
  let shared: Promise<T> | undefined;
  return () => {
    if (shared !== undefined) return shared;
    const building = create();
    shared = building;
    building.then(undefined, () => {
      if (shared === building) shared = undefined;
    });
    return building;
  };
}
