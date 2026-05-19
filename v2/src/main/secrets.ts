import { safeStorage } from 'electron';

let warned = false;

function warnIfPlaintext(): void {
  if (warned) return;
  if (!safeStorage.isEncryptionAvailable()) {
    // eslint-disable-next-line no-console
    console.warn(
      '[secrets] safeStorage encryption is NOT available on this platform — ' +
        'connection secrets will be persisted as plaintext. On Linux this ' +
        'usually means libsecret / gnome-keyring is missing.',
    );
    warned = true;
  }
}

export function encryptString(plain: string): Buffer {
  warnIfPlaintext();
  if (!safeStorage.isEncryptionAvailable()) {
    return Buffer.from(plain, 'utf8');
  }
  return safeStorage.encryptString(plain);
}

export function decryptString(cipher: Buffer): string {
  if (!safeStorage.isEncryptionAvailable()) {
    return cipher.toString('utf8');
  }
  return safeStorage.decryptString(cipher);
}
