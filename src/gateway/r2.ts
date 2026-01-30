import type { Sandbox } from '@cloudflare/sandbox';
import type { OpenClawEnv } from '../types';
import { R2_MOUNT_PATH, R2_BUCKET_NAME } from '../config';
import { waitForProcess } from './utils';

/**
 * Check if R2 is already mounted by looking at the mount table.
 *
 * Important: sandbox process status can lag behind actual completion, so we
 * must not treat a `running` status as authoritative for small commands.
 */
async function isR2Mounted(sandbox: Sandbox): Promise<boolean> {
  try {
    // Prefer /proc/mounts: stable format and no shell needed.
    const proc = await sandbox.startProcess('cat /proc/mounts');

    // Best-effort wait: status may lag, so ignore timeouts.
    await waitForProcess(proc, 5000, 200).catch(() => {});

    // Poll logs briefly in case status/logs lag behind each other.
    let stdout = '';
    for (let i = 0; i < 10; i++) {
      const logs = await proc.getLogs().catch(() => ({ stdout: '', stderr: '' }));
      stdout = logs.stdout || '';
      if (stdout.trim().length > 0) break;
      await new Promise((r) => setTimeout(r, 100));
    }

    const lines = stdout.split('\n');
    // `mount` output uses "on <path>", /proc/mounts uses "<path>".
    const mounted = lines.some((line) => {
      if (!line.includes('s3fs')) return false;
      return line.includes(` ${R2_MOUNT_PATH} `) || line.includes(` on ${R2_MOUNT_PATH} `);
    });

    console.log('isR2Mounted check:', mounted);
    return mounted;
  } catch (err) {
    console.log('isR2Mounted error:', err);
    return false;
  }
}

/**
 * Mount R2 bucket for persistent storage
 * 
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns true if mounted successfully, false otherwise
 */
export async function mountR2Storage(sandbox: Sandbox, env: OpenClawEnv): Promise<boolean> {
  // Skip if R2 credentials are not configured
  if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY || !env.CF_ACCOUNT_ID) {
    console.log('R2 storage not configured (missing R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, or CF_ACCOUNT_ID)');
    return false;
  }

  // Check if already mounted first - this avoids errors and is faster
  if (await isR2Mounted(sandbox)) {
    console.log('R2 bucket already mounted at', R2_MOUNT_PATH);
    return true;
  }

  try {
    console.log('Mounting R2 bucket at', R2_MOUNT_PATH);
    await sandbox.mountBucket(R2_BUCKET_NAME, R2_MOUNT_PATH, {
      endpoint: `https://${env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      // Pass credentials explicitly since we use R2_* naming instead of AWS_*
      credentials: {
        accessKeyId: env.R2_ACCESS_KEY_ID,
        secretAccessKey: env.R2_SECRET_ACCESS_KEY,
      },
    });
    console.log('R2 bucket mounted successfully - openclaw data will persist across sessions');
    return true;
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    console.log('R2 mount error:', errorMessage);
    
    // Check again if it's mounted - the error might be misleading
    if (await isR2Mounted(sandbox)) {
      console.log('R2 bucket is mounted despite error');
      return true;
    }
    
    // Don't fail if mounting fails - openclaw can still run without persistent storage
    console.error('Failed to mount R2 bucket:', err);
    return false;
  }
}
