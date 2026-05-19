import type { Document, MongoClient } from 'mongodb';
import type { ValidatorPayload } from '../../shared/schema.js';

export async function getValidator(
  client: MongoClient,
  db: string,
  collection: string,
): Promise<ValidatorPayload> {
  const list = await client
    .db(db)
    .listCollections({ name: collection }, { nameOnly: false })
    .toArray();
  if (list.length === 0) return { validator: null };
  const opts = ((list[0] as Document)['options'] ?? {}) as Document;
  const v = opts['validator'];
  return {
    validator: v ? JSON.stringify(v, null, 2) : null,
    validationLevel: opts['validationLevel'] as ValidatorPayload['validationLevel'],
    validationAction: opts['validationAction'] as ValidatorPayload['validationAction'],
  };
}

export async function setValidator(
  client: MongoClient,
  db: string,
  collection: string,
  payload: ValidatorPayload,
): Promise<void> {
  const cmd: Document = { collMod: collection };
  if (payload.validator) {
    try {
      cmd['validator'] = JSON.parse(payload.validator);
    } catch (e) {
      throw new Error(`Validator is not valid JSON: ${(e as Error).message}`);
    }
  } else {
    cmd['validator'] = {};
  }
  if (payload.validationLevel) cmd['validationLevel'] = payload.validationLevel;
  if (payload.validationAction) cmd['validationAction'] = payload.validationAction;
  await client.db(db).command(cmd);
}
