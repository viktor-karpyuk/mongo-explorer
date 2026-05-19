export const IpcChannels = {
  AppVersion: 'app:version',
  AppPing: 'app:ping',
} as const;

export type IpcChannel = (typeof IpcChannels)[keyof typeof IpcChannels];

export interface BridgeApi {
  appVersion: () => Promise<string>;
  ping: () => Promise<'pong'>;
}

declare global {
  interface Window {
    mex: BridgeApi;
  }
}
