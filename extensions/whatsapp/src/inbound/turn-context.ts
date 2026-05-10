import { AsyncLocalStorage } from "node:async_hooks";

// Carries the trigger message's quote metadata so outbound sends issued
// during the same agent turn — in particular via the `message` tool path —
// can auto-quote it. Upstream PR #62305 wired replyToId through the reply
// payload for deliverWebReply, but the `message` tool → sendMessageWhatsApp
// → sendApi.sendMessage path has no payload to carry replyToId on, so
// stickers/images shipped via the tool went out without a quote. This fills
// that gap.
//
// Propagates via Node async hooks so any awaits/tool calls in the same turn
// inherit the context. Group-only: DMs never auto-quote.
export type WhatsAppTurnQuoteKey = {
  id: string;
  remoteJid: string;
  fromMe: boolean;
  participant?: string;
  messageText?: string;
};

const turnContextStorage = new AsyncLocalStorage<WhatsAppTurnQuoteKey>();

export function runWithWhatsAppTurnQuoteKey<T>(
  key: WhatsAppTurnQuoteKey,
  fn: () => Promise<T>,
): Promise<T> {
  return turnContextStorage.run(key, fn);
}

export function getWhatsAppTurnQuoteKey(): WhatsAppTurnQuoteKey | undefined {
  return turnContextStorage.getStore();
}
