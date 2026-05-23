import { execFile, type ExecFileException, type ExecFileOptions } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { formatCliCommand } from "openclaw/plugin-sdk/cli-runtime";
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import { generateSecureUuid } from "openclaw/plugin-sdk/core";
import { redactIdentifier } from "openclaw/plugin-sdk/logging-core";
import {
  convertMarkdownTables,
  resolveMarkdownTableMode,
} from "openclaw/plugin-sdk/markdown-table-runtime";
import { requireRuntimeConfig } from "openclaw/plugin-sdk/plugin-config-runtime";
import { normalizePollInput, type PollInput } from "openclaw/plugin-sdk/poll-runtime";
import { createSubsystemLogger, getChildLogger } from "openclaw/plugin-sdk/runtime-env";
import {
  resolveDefaultWhatsAppAccountId,
  resolveWhatsAppAccount,
  resolveWhatsAppMediaMaxBytes,
} from "./accounts.js";
import { getRegisteredWhatsAppConnectionController } from "./connection-controller-registry.js";
import { resolveWhatsAppDocumentFileName } from "./document-filename.js";
import type { ActiveWebListener, ActiveWebSendOptions } from "./inbound/types.js";
import { isWhatsAppNewsletterJid } from "./normalize.js";
import {
  normalizeWhatsAppPayloadText,
  prepareWhatsAppOutboundMedia,
  resolveWhatsAppOutboundMediaUrls,
} from "./outbound-media-contract.js";
import { loadOutboundMediaFromUrl } from "./outbound-media.runtime.js";
import { markdownToWhatsApp, toWhatsappJid } from "./text-runtime.js";

const outboundLog = createSubsystemLogger("gateway/channels/whatsapp").child("outbound");
const STICKER_MARK_SENT_TIMEOUT_MS = 2000;

function supportsForcedDocumentDelivery(kind: "image" | "audio" | "video" | "document"): boolean {
  return kind === "image" || kind === "video";
}

function resolveOutboundWhatsAppAccountId(params: {
  cfg: OpenClawConfig;
  accountId?: string;
}): string | undefined {
  const explicitAccountId = params.accountId?.trim();
  if (explicitAccountId) {
    return explicitAccountId;
  }
  return resolveDefaultWhatsAppAccountId(params.cfg);
}

function requireOutboundActiveWebListener(params: { cfg: OpenClawConfig; accountId?: string }): {
  accountId: string;
  listener: ActiveWebListener;
} {
  const accountId = resolveOutboundWhatsAppAccountId(params);
  const resolvedAccountId = accountId ?? resolveDefaultWhatsAppAccountId(params.cfg);
  const listener =
    getRegisteredWhatsAppConnectionController(resolvedAccountId)?.getActiveListener() ?? null;
  if (!listener) {
    throw new Error(
      `No active WhatsApp Web listener (account: ${resolvedAccountId}). Start the gateway, then link WhatsApp with: ${formatCliCommand(`openclaw channels login --channel whatsapp --account ${resolvedAccountId}`)}.`,
    );
  }
  return { accountId: resolvedAccountId, listener };
}

function resolveGringoNgrBin(): string {
  return process.env.GRINGO_NGR_BIN?.trim() || process.env.NGR_BIN?.trim() || "ngr";
}

function resolveGringoWorkspaceCwd(): string | undefined {
  return (
    process.env.GRINGO_WORKSPACE_DIR?.trim() || process.env.OPENCLAW_GRINGO_WORKSPACE_DIR?.trim()
  );
}

function resolveLocalMediaPath(mediaUrl: string | undefined): string | undefined {
  const trimmed = mediaUrl?.trim();
  if (!trimmed) {
    return undefined;
  }
  try {
    const parsed = new URL(trimmed);
    return parsed.protocol === "file:" ? fileURLToPath(parsed) : undefined;
  } catch {
    const withoutQueryOrFragment = trimmed.split(/[?#]/, 1)[0] ?? "";
    return path.isAbsolute(withoutQueryOrFragment) ? withoutQueryOrFragment : undefined;
  }
}

function formatStickerMarkSentError(error: ExecFileException | null, stderr: string): string {
  if (error) {
    return error.killed || error.signal
      ? `process ${error.signal ?? "killed"}`
      : error.message || "process failed";
  }
  return stderr.trim() || "no output";
}

function markStickerSentAfterDelivery(params: {
  mediaUrl?: string;
  mediaType?: string;
  providerAccepted: boolean;
}) {
  if (!params.providerAccepted || params.mediaType !== "image/webp") {
    return;
  }
  const stickerPath = resolveLocalMediaPath(params.mediaUrl);
  if (!stickerPath) {
    return;
  }

  const execOptions: ExecFileOptions = {
    encoding: "utf8",
    env: {
      ...process.env,
      OPENCLAW_SHELL: process.env.OPENCLAW_SHELL?.trim() || "exec",
    },
    maxBuffer: 32 * 1024,
    timeout: STICKER_MARK_SENT_TIMEOUT_MS,
  };
  const cwd = resolveGringoWorkspaceCwd();
  if (cwd) {
    execOptions.cwd = cwd;
  }

  execFile(
    resolveGringoNgrBin(),
    ["stickers", "mark-sent", "--path", stickerPath, "--format", "json"],
    execOptions,
    (error: ExecFileException | null, _stdoutRaw: string | Buffer, stderrRaw: string | Buffer) => {
      if (!error) {
        return;
      }
      outboundLog.warn(
        `sticker mark-sent failed file=${path.basename(stickerPath)} error=${formatStickerMarkSentError(
          error,
          stderrRaw.toString(),
        )}`,
      );
    },
  );
}

export async function sendMessageWhatsApp(
  to: string,
  body: string,
  options: {
    verbose: boolean;
    cfg: OpenClawConfig;
    mediaUrl?: string;
    mediaUrls?: readonly string[];
    mediaAccess?: {
      localRoots?: readonly string[];
      readFile?: (filePath: string) => Promise<Buffer>;
    };
    mediaLocalRoots?: readonly string[];
    mediaReadFile?: (filePath: string) => Promise<Buffer>;
    gifPlayback?: boolean;
    audioAsVoice?: boolean;
    forceDocument?: boolean;
    accountId?: string;
    quotedMessageKey?: {
      id: string;
      remoteJid: string;
      fromMe: boolean;
      participant?: string;
      messageText?: string;
    };
    preserveLeadingWhitespace?: boolean;
  },
): Promise<{ messageId: string; toJid: string }> {
  let text = options.preserveLeadingWhitespace ? body : normalizeWhatsAppPayloadText(body);
  const jid = toWhatsappJid(to);
  const mediaUrls = resolveWhatsAppOutboundMediaUrls(options);
  const primaryMediaUrl = mediaUrls[0];
  if (!text && !primaryMediaUrl) {
    return { messageId: "", toJid: jid };
  }
  const correlationId = generateSecureUuid();
  const startedAt = Date.now();
  const cfg = requireRuntimeConfig(options.cfg, "WhatsApp send");
  const { listener: active, accountId: resolvedAccountId } = requireOutboundActiveWebListener({
    cfg,
    accountId: options.accountId,
  });
  const account = resolveWhatsAppAccount({
    cfg,
    accountId: resolvedAccountId ?? options.accountId,
  });
  const tableMode = resolveMarkdownTableMode({
    cfg,
    channel: "whatsapp",
    accountId: resolvedAccountId ?? options.accountId,
  });
  text = convertMarkdownTables(text ?? "", tableMode);
  text = markdownToWhatsApp(text);
  const redactedTo = redactIdentifier(to);
  const logger = getChildLogger({
    module: "web-outbound",
    correlationId,
    to: redactedTo,
  });
  try {
    const redactedJid = redactIdentifier(jid);
    let mediaBuffer: Buffer | undefined;
    let mediaType: string | undefined;
    let documentFileName: string | undefined;
    let visibleTextAfterVoice: string | undefined;
    let forceDocumentDelivery = false;
    if (primaryMediaUrl) {
      const media = await prepareWhatsAppOutboundMedia(
        await loadOutboundMediaFromUrl(primaryMediaUrl, {
          maxBytes: resolveWhatsAppMediaMaxBytes(account),
          optimizeImages: options.forceDocument ? false : undefined,
          mediaAccess: options.mediaAccess,
          mediaLocalRoots: options.mediaLocalRoots,
          mediaReadFile: options.mediaReadFile,
        }),
        primaryMediaUrl,
      );
      const caption = text || undefined;
      mediaBuffer = media.buffer;
      mediaType = media.mimetype;
      forceDocumentDelivery = Boolean(
        options.forceDocument && supportsForcedDocumentDelivery(media.kind),
      );
      if (media.kind === "audio" && caption) {
        visibleTextAfterVoice = caption;
        text = "";
      } else if (media.kind === "document") {
        text = caption ?? "";
        documentFileName = media.fileName;
      } else {
        text = caption ?? "";
      }
      if (forceDocumentDelivery) {
        documentFileName ??= resolveWhatsAppDocumentFileName({
          fileName: media.fileName,
          mimetype: media.mimetype,
        });
      }
    }
    outboundLog.info(`Sending message -> ${redactedJid}${primaryMediaUrl ? " (media)" : ""}`);
    logger.info({ jid: redactedJid, hasMedia: Boolean(primaryMediaUrl) }, "sending message");
    if (!primaryMediaUrl && !isWhatsAppNewsletterJid(jid)) {
      await active.sendComposingTo(to);
    }
    const hasExplicitAccountId = Boolean(options.accountId?.trim());
    const accountId = hasExplicitAccountId ? resolvedAccountId : undefined;
    const sendOptions: ActiveWebSendOptions | undefined =
      options.gifPlayback ||
      forceDocumentDelivery ||
      accountId ||
      documentFileName ||
      options.quotedMessageKey
        ? {
            ...(options.gifPlayback ? { gifPlayback: true } : {}),
            ...(forceDocumentDelivery ? { asDocument: true } : {}),
            ...(documentFileName ? { fileName: documentFileName } : {}),
            ...(options.quotedMessageKey ? { quotedMessageKey: options.quotedMessageKey } : {}),
            accountId,
          }
        : undefined;
    const result = sendOptions
      ? await active.sendMessage(to, text, mediaBuffer, mediaType, sendOptions)
      : await active.sendMessage(to, text, mediaBuffer, mediaType);
    markStickerSentAfterDelivery({
      mediaUrl: primaryMediaUrl,
      mediaType,
      providerAccepted: Boolean((result as { providerAccepted?: boolean })?.providerAccepted),
    });
    if (visibleTextAfterVoice) {
      if (sendOptions) {
        await active.sendMessage(to, visibleTextAfterVoice, undefined, undefined, sendOptions);
      } else {
        await active.sendMessage(to, visibleTextAfterVoice, undefined, undefined);
      }
    }
    const messageId = (result as { messageId?: string })?.messageId ?? "unknown";
    const durationMs = Date.now() - startedAt;
    outboundLog.info(
      `Sent message ${messageId} -> ${redactedJid}${primaryMediaUrl ? " (media)" : ""} (${durationMs}ms)`,
    );
    logger.info({ jid: redactedJid, messageId }, "sent message");
    return { messageId, toJid: jid };
  } catch (err) {
    logger.error(
      { err: String(err), to: redactedTo, hasMedia: Boolean(primaryMediaUrl) },
      "failed to send via web session",
    );
    throw err;
  }
}

export async function sendTypingWhatsApp(
  to: string,
  options: {
    cfg: OpenClawConfig;
    accountId?: string;
  },
): Promise<void> {
  const cfg = requireRuntimeConfig(options.cfg, "WhatsApp typing send");
  const { listener: active } = requireOutboundActiveWebListener({
    cfg,
    accountId: options.accountId,
  });
  if (!isWhatsAppNewsletterJid(toWhatsappJid(to))) {
    await active.sendComposingTo(to);
  }
}

export async function sendReactionWhatsApp(
  chatJid: string,
  messageId: string,
  emoji: string,
  options: {
    verbose: boolean;
    fromMe?: boolean;
    participant?: string;
    accountId?: string;
    cfg: OpenClawConfig;
  },
): Promise<void> {
  const correlationId = generateSecureUuid();
  const cfg = requireRuntimeConfig(options.cfg, "WhatsApp reaction");
  const { listener: active } = requireOutboundActiveWebListener({
    cfg,
    accountId: options.accountId,
  });
  const redactedChatJid = redactIdentifier(chatJid);
  const logger = getChildLogger({
    module: "web-outbound",
    correlationId,
    chatJid: redactedChatJid,
    messageId,
  });
  try {
    const jid = toWhatsappJid(chatJid);
    const redactedJid = redactIdentifier(jid);
    outboundLog.info(`Sending reaction "${emoji}" -> message ${messageId}`);
    logger.info({ chatJid: redactedJid, messageId, emoji }, "sending reaction");
    await active.sendReaction(
      chatJid,
      messageId,
      emoji,
      options.fromMe ?? false,
      options.participant,
    );
    outboundLog.info(`Sent reaction "${emoji}" -> message ${messageId}`);
    logger.info({ chatJid: redactedJid, messageId, emoji }, "sent reaction");
  } catch (err) {
    logger.error(
      { err: String(err), chatJid: redactedChatJid, messageId, emoji },
      "failed to send reaction via web session",
    );
    throw err;
  }
}

export async function sendPollWhatsApp(
  to: string,
  poll: PollInput,
  options: { verbose: boolean; accountId?: string; cfg: OpenClawConfig },
): Promise<{ messageId: string; toJid: string }> {
  const correlationId = generateSecureUuid();
  const startedAt = Date.now();
  const cfg = requireRuntimeConfig(options.cfg, "WhatsApp poll");
  const { listener: active } = requireOutboundActiveWebListener({
    cfg,
    accountId: options.accountId,
  });
  const redactedTo = redactIdentifier(to);
  const logger = getChildLogger({
    module: "web-outbound",
    correlationId,
    to: redactedTo,
  });
  try {
    const jid = toWhatsappJid(to);
    const redactedJid = redactIdentifier(jid);
    const normalized = normalizePollInput(poll, { maxOptions: 12 });
    outboundLog.info(`Sending poll -> ${redactedJid}`);
    logger.info(
      {
        jid: redactedJid,
        optionCount: normalized.options.length,
        maxSelections: normalized.maxSelections,
      },
      "sending poll",
    );
    const result = await active.sendPoll(to, normalized);
    const messageId = (result as { messageId?: string })?.messageId ?? "unknown";
    const durationMs = Date.now() - startedAt;
    outboundLog.info(`Sent poll ${messageId} -> ${redactedJid} (${durationMs}ms)`);
    logger.info({ jid: redactedJid, messageId }, "sent poll");
    return { messageId, toJid: jid };
  } catch (err) {
    logger.error({ err: String(err), to: redactedTo }, "failed to send poll via web session");
    throw err;
  }
}
