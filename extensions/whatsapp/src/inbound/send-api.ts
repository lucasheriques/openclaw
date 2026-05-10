import type {
  AnyMessageContent,
  MiscMessageGenerationOptions,
  WAMessage,
  WAPresence,
} from "baileys";
import { recordChannelActivity } from "openclaw/plugin-sdk/channel-activity-runtime";
import { resolveWhatsAppDocumentFileName } from "../document-filename.js";
import { redactIdentifier } from "openclaw/plugin-sdk/logging-core";
import { createSubsystemLogger } from "openclaw/plugin-sdk/runtime-env";
import { isWhatsAppNewsletterJid } from "../normalize.js";
import { buildQuotedMessageOptions } from "../quoted-message.js";
import { toWhatsappJid, toWhatsappJidWithLid } from "../text-runtime.js";
import {
  addWhatsAppOutboundMentionsToContent,
  type WhatsAppOutboundMentionResolution,
} from "./outbound-mentions.js";
import {
  combineWhatsAppSendResults,
  normalizeWhatsAppSendResult,
  type WhatsAppSendResult,
} from "./send-result.js";
import { getWhatsAppTurnQuoteKey } from "./turn-context.js";
import type { ActiveWebSendOptions } from "./types.js";

const sendApiLog = createSubsystemLogger("gateway/channels/whatsapp").child("outbound");

function recordWhatsAppOutbound(accountId: string) {
  recordChannelActivity({
    channel: "whatsapp",
    accountId,
    direction: "outbound",
  });
}

function supportsForcedDocumentMediaType(mediaType: string): boolean {
  return mediaType.startsWith("image/") || mediaType.startsWith("video/");
}

function payloadKind(payload: AnyMessageContent): string {
  return Object.keys(payload)[0] ?? "unknown";
}

function redactOptionalIdentifier(value: string | undefined): string {
  return value ? redactIdentifier(value) : "none";
}

function isSafeQuotedMessageKeyForTarget(
  jid: string,
  quoteKey: ActiveWebSendOptions["quotedMessageKey"] | undefined,
): boolean {
  if (!quoteKey) {
    return false;
  }
  if (!jid.endsWith("@g.us")) {
    return true;
  }
  if (quoteKey.fromMe) {
    return true;
  }
  return Boolean(quoteKey.participant?.trim() && quoteKey.messageText?.trim());
}

export function createWebSendApi(params: {
  sock: {
    sendMessage: (
      jid: string,
      content: AnyMessageContent,
      options?: MiscMessageGenerationOptions,
    ) => Promise<WAMessage | undefined>;
    sendPresenceUpdate: (presence: WAPresence, jid?: string) => Promise<unknown>;
  };
  defaultAccountId: string;
  resolveOutboundMentions?: (params: {
    jid: string;
    text: string;
  }) => Promise<WhatsAppOutboundMentionResolution> | WhatsAppOutboundMentionResolution;
  // When provided, lets outbound resolve `{phone}@s.whatsapp.net` to `{lid}@lid`
  // via Baileys' lid-mapping-{phone-digits}.json files in the auth dir, so
  // proactive sends to LID-addressed contacts reach the recipient instead of
  // ending up in a sender-only ghost chat (#67378). Defaults to PN-only.
  authDir?: string;
}) {
  const resolveOutboundJid = (recipient: string): string =>
    params.authDir
      ? toWhatsappJidWithLid(recipient, { authDir: params.authDir })
      : toWhatsappJid(recipient);
  const resolveMentions = async (
    jid: string,
    text: string,
  ): Promise<WhatsAppOutboundMentionResolution> =>
    params.resolveOutboundMentions
      ? await params.resolveOutboundMentions({ jid, text })
      : { text, mentionedJids: [] };

  return {
    sendMessage: async (
      to: string,
      text: string,
      mediaBuffer?: Buffer,
      mediaType?: string,
      sendOptions?: ActiveWebSendOptions,
    ): Promise<WhatsAppSendResult> => {
      const jid = resolveOutboundJid(to);
      let payload: AnyMessageContent;
      if (mediaBuffer) {
        mediaType ??= "application/octet-stream";
      }
      const shouldSendAudioText = Boolean(
        mediaBuffer && mediaType?.startsWith("audio/") && text.trim(),
      );
      const resolvedPayloadText = shouldSendAudioText
        ? { text, mentionedJids: [] }
        : await resolveMentions(jid, text);
      if (mediaBuffer && mediaType) {
        if (sendOptions?.asDocument === true && supportsForcedDocumentMediaType(mediaType)) {
          const fileName = resolveWhatsAppDocumentFileName({
            fileName: sendOptions?.fileName,
            mimetype: mediaType,
          });
          payload = {
            document: mediaBuffer,
            fileName,
            caption: resolvedPayloadText.text || undefined,
            mimetype: mediaType,
          };
        } else if (mediaType === "image/webp") {
          payload = { sticker: mediaBuffer };
        } else if (mediaType.startsWith("image/")) {
          payload = {
            image: mediaBuffer,
            caption: resolvedPayloadText.text || undefined,
            mimetype: mediaType,
          };
        } else if (mediaType.startsWith("audio/")) {
          payload = { audio: mediaBuffer, ptt: true, mimetype: mediaType };
        } else if (mediaType.startsWith("video/")) {
          const gifPlayback = sendOptions?.gifPlayback;
          payload = {
            video: mediaBuffer,
            caption: resolvedPayloadText.text || undefined,
            mimetype: mediaType,
            ...(gifPlayback ? { gifPlayback: true } : {}),
          };
        } else {
          const fileName = resolveWhatsAppDocumentFileName({
            fileName: sendOptions?.fileName,
            mimetype: mediaType,
          });
          payload = {
            document: mediaBuffer,
            fileName,
            caption: resolvedPayloadText.text || undefined,
            mimetype: mediaType,
          };
        }
      } else {
        payload = { text: resolvedPayloadText.text };
      }
      payload = addWhatsAppOutboundMentionsToContent(payload, resolvedPayloadText.mentionedJids);
      const explicitQuoteKey = sendOptions?.quotedMessageKey;
      const ambientQuoteKey = getWhatsAppTurnQuoteKey();
      const ambientMatchesExplicit =
        Boolean(explicitQuoteKey) &&
        ambientQuoteKey?.id === explicitQuoteKey?.id &&
        ambientQuoteKey?.remoteJid === explicitQuoteKey?.remoteJid;
      const ambientMatchesTarget = ambientQuoteKey?.remoteJid === jid;
      const effectiveQuoteKey =
        (ambientMatchesExplicit ? ambientQuoteKey : undefined) ??
        explicitQuoteKey ??
        (ambientMatchesTarget ? ambientQuoteKey : undefined);
      const quoteDropped =
        effectiveQuoteKey && !isSafeQuotedMessageKeyForTarget(jid, effectiveQuoteKey);
      const quoteSource = ambientMatchesExplicit
        ? "ambient-matching-explicit"
        : explicitQuoteKey
          ? "explicit"
          : ambientMatchesTarget
            ? "ambient-target"
            : "none";
      const quotedOpts = buildQuotedMessageOptions({
        messageId: quoteDropped ? undefined : effectiveQuoteKey?.id,
        remoteJid: quoteDropped ? undefined : effectiveQuoteKey?.remoteJid,
        fromMe: quoteDropped ? undefined : effectiveQuoteKey?.fromMe,
        participant: quoteDropped ? undefined : effectiveQuoteKey?.participant,
        messageText: quoteDropped ? undefined : effectiveQuoteKey?.messageText,
      });
      sendApiLog.info(
        [
          `Baileys send -> ${redactIdentifier(jid)}`,
          `payload=${payloadKind(payload)}`,
          `quote=${quotedOpts ? "yes" : "no"}`,
          `quoteSource=${quoteSource}`,
          `quoteId=${effectiveQuoteKey?.id ?? "none"}`,
          `quoteRemote=${redactOptionalIdentifier(effectiveQuoteKey?.remoteJid)}`,
          `quoteParticipant=${redactOptionalIdentifier(effectiveQuoteKey?.participant)}`,
          `quoteFromMe=${effectiveQuoteKey?.fromMe ?? false}`,
          `hasQuoteText=${Boolean(effectiveQuoteKey?.messageText)}`,
          `quoteDropped=${quoteDropped ? "unsafe-group-quote" : "no"}`,
        ].join(" "),
      );
      const result = quotedOpts
        ? await params.sock.sendMessage(jid, payload, quotedOpts)
        : await params.sock.sendMessage(jid, payload);
      const results = [normalizeWhatsAppSendResult(result, mediaBuffer ? "media" : "text")];
      if (shouldSendAudioText) {
        const resolvedAudioText = await resolveMentions(jid, text);
        const textPayload = addWhatsAppOutboundMentionsToContent(
          { text: resolvedAudioText.text },
          resolvedAudioText.mentionedJids,
        );
        const textResult = quotedOpts
          ? await params.sock.sendMessage(jid, textPayload, quotedOpts)
          : await params.sock.sendMessage(jid, textPayload);
        results.push(normalizeWhatsAppSendResult(textResult, "text"));
      }
      const accountId = sendOptions?.accountId ?? params.defaultAccountId;
      recordWhatsAppOutbound(accountId);
      return combineWhatsAppSendResults(mediaBuffer ? "media" : "text", results);
    },
    sendPoll: async (
      to: string,
      poll: { question: string; options: string[]; maxSelections?: number },
    ): Promise<WhatsAppSendResult> => {
      const jid = resolveOutboundJid(to);
      const result = await params.sock.sendMessage(jid, {
        poll: {
          name: poll.question,
          values: poll.options,
          selectableCount: poll.maxSelections ?? 1,
        },
      } as AnyMessageContent);
      recordWhatsAppOutbound(params.defaultAccountId);
      return normalizeWhatsAppSendResult(result, "poll");
    },
    sendReaction: async (
      chatJid: string,
      messageId: string,
      emoji: string,
      fromMe: boolean,
      participant?: string,
    ): Promise<WhatsAppSendResult> => {
      // chatJid is typically already a JID (group or DM); pass through
      // unchanged. The participant is a sender id and stays PN-shaped to match
      // how the existing inbound flow stores it.
      const jid = toWhatsappJid(chatJid);
      const result = await params.sock.sendMessage(jid, {
        react: {
          text: emoji,
          key: {
            remoteJid: jid,
            id: messageId,
            fromMe,
            participant: participant ? toWhatsappJid(participant) : undefined,
          },
        },
      } as AnyMessageContent);
      return normalizeWhatsAppSendResult(result, "reaction");
    },
    sendComposingTo: async (to: string): Promise<void> => {
      const jid = resolveOutboundJid(to);
      if (isWhatsAppNewsletterJid(jid)) {
        return;
      }
      await params.sock.sendPresenceUpdate("composing", jid);
    },
  } as const;
}
