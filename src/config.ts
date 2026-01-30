/**
 * Configuration constants for OpenClaw Sandbox
 */

/** Port that the OpenClaw gateway listens on inside the container */
export const MOLTBOT_PORT = 18789;

/** Maximum time to wait for OpenClaw to start (3 minutes) */
export const STARTUP_TIMEOUT_MS = 180_000;

/** Mount path for R2 persistent storage inside the container */
export const R2_MOUNT_PATH = '/data/openclaw';

/** R2 bucket name for persistent storage */
export const R2_BUCKET_NAME = 'openclaw-data';
