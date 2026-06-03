import { startServices } from "./fixtures/services";

/**
 * Spawn the service binaries before the suite runs. Pids are persisted to
 * .e2e-state.json so globalTeardown can stop them even across process
 * boundaries.
 */
export default async function globalSetup(): Promise<void> {
  await startServices();
}
