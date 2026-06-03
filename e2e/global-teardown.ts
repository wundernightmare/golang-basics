import { stopServices } from "./fixtures/services";

/** Stop every service spawned by globalSetup. */
export default async function globalTeardown(): Promise<void> {
  stopServices();
}
