import { execFile, type ExecFileException, type ExecFileOptions } from "node:child_process";
import {
  logAckFailure,
  removeAckReactionHandleAfterReply,
  type AckReactionHandle,
} from "openclaw/plugin-sdk/channel-feedback";
import type { CommandTurnContext } from "openclaw/plugin-sdk/channel-inbound";
import { recordInboundSession } from "openclaw/plugin-sdk/conversation-runtime";
import {
  createInternalHookEvent,
  deriveInboundMessageHookContext,
  fireAndForgetBoundedHook,
  toInternalMessageReceivedContext,
  toPluginMessageContext,
  toPluginMessageReceivedEvent,
  triggerInternalHook,
} from "openclaw/plugin-sdk/hook-runtime";
import { runInboundReplyTurn } from "openclaw/plugin-sdk/inbound-reply-dispatch";
import { getGlobalHookRunner } from "openclaw/plugin-sdk/plugin-runtime";
import { resolveBatchedReplyThreadingPolicy } from "openclaw/plugin-sdk/reply-reference";
import { getPrimaryIdentityId, getSelfIdentity, getSenderIdentity } from "../../identity.js";
import {
  resolveWhatsAppCommandAuthorized,
  resolveWhatsAppInboundPolicy,
} from "../../inbound-policy.js";
import { newConnectionId } from "../../reconnect.js";
import { formatError } from "../../session.js";
import {
  resolveWhatsAppDirectSystemPrompt,
  resolveWhatsAppGroupSystemPrompt,
} from "../../system-prompt.js";
import { deliverWebReply } from "../deliver-reply.js";
import { whatsappInboundLog } from "../loggers.js";
import type { WebInboundMsg } from "../types.js";
import { elide } from "../util.js";
import { maybeSendAckReaction } from "./ack-reaction.js";
import {
  resolveVisibleWhatsAppGroupHistory,
  resolveVisibleWhatsAppReplyContext,
  type GroupHistoryEntry,
} from "./inbound-context.js";
import {
  buildWhatsAppInboundContext,
  dispatchWhatsAppBufferedReply,
  resolveWhatsAppDmRouteTarget,
  resolveWhatsAppResponsePrefix,
  updateWhatsAppMainLastRoute,
} from "./inbound-dispatch.js";
import { trackBackgroundTask, updateLastRouteInBackground } from "./last-route.js";
import { buildInboundLine } from "./message-line.js";
import {
  buildHistoryContextFromEntries,
  createChannelMessageReplyPipeline,
  formatInboundEnvelope,
  logVerbose,
  normalizeE164,
  resolveChannelContextVisibilityMode,
  resolveInboundSessionEnvelopeContext,
  resolvePinnedMainDmOwnerFromAllowlist,
  isControlCommandMessage,
  shouldComputeCommandAuthorized,
  shouldLogVerbose,
  type getChildLogger,
  type getReplyFromConfig,
  type HistoryEntry,
  type LoadConfigFn,
  type resolveAgentRoute,
} from "./runtime-api.js";
import {
  createWhatsAppStatusReactionController,
  type StatusReactionController,
} from "./status-reaction.js";

const WHATSAPP_MESSAGE_RECEIVED_HOOK_LIMITS = {
  maxConcurrency: 8,
  maxQueue: 128,
  timeoutMs: 2_000,
};

const GRINGO_AGENT_ID = "gringo";
const GRINGO_IDENTITY_PRELOAD_DEFAULT_TIMEOUT_MS = 2_500;
const GRINGO_IDENTITY_PRELOAD_SESSION_TTL_MS = 2 * 60 * 60 * 1000;
const GRINGO_IDENTITY_PRELOAD_SESSION_MAX_ENTRIES = 1000;
const gringoIdentityPreloadedDmSessions = new Map<
  string,
  { workingContext: string; cachedAtMs: number }
>();

type WhatsAppMessageReceivedHookConfig = {
  pluginHooks?: {
    messageReceived?: unknown;
  };
  accounts?: Record<string, unknown>;
};

type GringoIdentityPreloadStatus =
  | "skipped"
  | "cached"
  | "group_hint"
  | "success"
  | "unknown"
  | "timeout"
  | "error";

type GringoIdentityPreloadResult = {
  status: GringoIdentityPreloadStatus;
  durationMs: number;
  bodyForAgentPrefix?: string;
  contextLength?: number;
  coachingContextLoaded?: boolean;
  coachingContextChars?: number;
  coachingContextVersion?: string;
  userProfileContextLoaded?: boolean;
  userProfileContextChars?: number;
  trustedContextChars?: number;
  openClawSessionKeyForwarded?: boolean;
  error?: string;
};

function resolveGringoIdentityPreloadTimeoutMs(): number {
  const raw = process.env.OPENCLAW_GRINGO_IDENTITY_PRELOAD_TIMEOUT_MS?.trim();
  if (!raw) {
    return GRINGO_IDENTITY_PRELOAD_DEFAULT_TIMEOUT_MS;
  }
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0
    ? parsed
    : GRINGO_IDENTITY_PRELOAD_DEFAULT_TIMEOUT_MS;
}

function resolveGringoNgrBin(): string {
  return process.env.GRINGO_NGR_BIN?.trim() || process.env.NGR_BIN?.trim() || "ngr";
}

function resolveGringoWorkspaceCwd(): string | undefined {
  return (
    process.env.GRINGO_WORKSPACE_DIR?.trim() || process.env.OPENCLAW_GRINGO_WORKSPACE_DIR?.trim()
  );
}

function isUnknownGringoUserContext(stdout: string): boolean {
  return /unknown user|not map to a known user/i.test(stdout);
}

function isProcessTimeoutError(error: unknown): boolean {
  return Boolean(
    error &&
    typeof error === "object" &&
    "killed" in error &&
    (error as { killed?: boolean }).killed,
  );
}

export function clearGringoIdentityPreloadSessionCacheForTests(): void {
  gringoIdentityPreloadedDmSessions.clear();
}

function shouldForceGringoIdentityPreload(text: string | undefined): boolean {
  if (!text) {
    return false;
  }
  return /(?:refresh|reload|atualiz|recarreg).*(?:context|perfil|assinatura)|(?:context|perfil|assinatura).*(?:refresh|reload|atualiz|recarreg)/i.test(
    text,
  );
}

function formatGringoCachedIdentityHint(): string {
  return "Trusted session context: loaded. Refresh only if asked.";
}

function formatGringoPreloadedIdentityHint(): string {
  return "Trusted context for this turn is preloaded. Do not reload profile/coaching files unless updating memory.";
}

function getCachedGringoIdentityWorkingContext(sessionCacheKey: string): string | undefined {
  const entry = gringoIdentityPreloadedDmSessions.get(sessionCacheKey);
  if (!entry) {
    return undefined;
  }
  if (Date.now() - entry.cachedAtMs > GRINGO_IDENTITY_PRELOAD_SESSION_TTL_MS) {
    gringoIdentityPreloadedDmSessions.delete(sessionCacheKey);
    return undefined;
  }
  return entry.workingContext;
}

function setCachedGringoIdentityWorkingContext(
  sessionCacheKey: string,
  workingContext: string,
): void {
  gringoIdentityPreloadedDmSessions.set(sessionCacheKey, {
    workingContext,
    cachedAtMs: Date.now(),
  });
  if (gringoIdentityPreloadedDmSessions.size <= GRINGO_IDENTITY_PRELOAD_SESSION_MAX_ENTRIES) {
    return;
  }
  for (const key of gringoIdentityPreloadedDmSessions.keys()) {
    gringoIdentityPreloadedDmSessions.delete(key);
    if (gringoIdentityPreloadedDmSessions.size <= GRINGO_IDENTITY_PRELOAD_SESSION_MAX_ENTRIES) {
      break;
    }
  }
}

function formatGringoGroupIdentityHint(params: { phone: string; senderJid?: string }): string {
  return [
    `Trusted WhatsApp group sender: ${params.phone}${params.senderJid ? ` (${params.senderJid})` : ""}.`,
    "Profile context is not preloaded in groups to keep the turn small. If the reply needs the sender's NaGringa profile, subscription/access, resume, jobs, or personalization, identify on demand with `ngr coach context --phone <trusted_phone> --surface group`; otherwise answer directly.",
  ].join("\n");
}

function readObject(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value : undefined;
}

function readBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function readNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function parseGringoCoachContextOutput(output: string): {
  prompt: string;
  workingContext?: string;
  coachingContextLoaded?: boolean;
  coachingContextChars?: number;
  coachingContextVersion?: string;
  userProfileContextLoaded?: boolean;
  userProfileContextChars?: number;
  trustedContextChars?: number;
} | null {
  try {
    const envelope = JSON.parse(output) as unknown;
    const data = readObject(readObject(envelope)?.data) ?? readObject(envelope);
    if (!data) {
      return null;
    }
    const prompt = readString(data.prompt);
    if (!prompt) {
      return null;
    }
    const coachingState = readObject(data.coachingState);
    const userProfile = readObject(data.userProfile);
    return {
      prompt,
      workingContext: readString(data.workingContext),
      coachingContextLoaded: readBoolean(coachingState?.loaded),
      coachingContextChars: readNumber(coachingState?.included),
      coachingContextVersion: readString(coachingState?.updatedAt),
      userProfileContextLoaded: readBoolean(userProfile?.loaded),
      userProfileContextChars: readNumber(userProfile?.included),
      trustedContextChars: prompt.length,
    };
  } catch {
    return null;
  }
}

async function preloadGringoIdentityContext(params: {
  agentId: string;
  chatType: "direct" | "group";
  sessionKey?: string;
  correlationId?: string;
  inboundText?: string;
  refreshText?: string;
  phone?: string;
  senderJid?: string;
  accountId?: string;
}): Promise<GringoIdentityPreloadResult> {
  const startedAt = Date.now();
  if (
    params.agentId !== GRINGO_AGENT_ID ||
    process.env.OPENCLAW_GRINGO_IDENTITY_PRELOAD === "0" ||
    !params.phone
  ) {
    return { status: "skipped", durationMs: 0 };
  }
  if (params.chatType === "group") {
    return {
      status: "group_hint",
      durationMs: Date.now() - startedAt,
      bodyForAgentPrefix: formatGringoGroupIdentityHint({
        phone: params.phone,
        senderJid: params.senderJid,
      }),
      contextLength: params.phone.length,
    };
  }

  const sessionCacheKey =
    params.sessionKey && process.env.OPENCLAW_GRINGO_IDENTITY_PRELOAD_EVERY_TURN !== "1"
      ? `${params.agentId}:${params.sessionKey}`
      : undefined;
  const cachedHint =
    sessionCacheKey && !shouldForceGringoIdentityPreload(params.refreshText ?? params.inboundText)
      ? getCachedGringoIdentityWorkingContext(sessionCacheKey)
      : undefined;
  if (sessionCacheKey && cachedHint) {
    return {
      status: "cached",
      durationMs: Date.now() - startedAt,
      bodyForAgentPrefix: cachedHint,
      contextLength: cachedHint.length,
      openClawSessionKeyForwarded: true,
    };
  }

  const phone = params.phone;
  const timeoutMs = resolveGringoIdentityPreloadTimeoutMs();
  const ngrBin = resolveGringoNgrBin();
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    OPENCLAW_SHELL: "exec",
    OPENCLAW_CALLER_CHANNEL: "whatsapp",
    OPENCLAW_CALLER_PHONE: phone,
    OPENCLAW_AGENT_ID: params.agentId,
    OPENCLAW_CHAT_TYPE: params.chatType,
  };
  if (params.correlationId) {
    env.OPENCLAW_CORRELATION_ID = params.correlationId;
    env.OPENCLAW_TURN_ID = params.correlationId;
  }
  if (params.inboundText) {
    env.OPENCLAW_INBOUND_TEXT = params.inboundText;
  }
  if (params.sessionKey) {
    env.OPENCLAW_SESSION_KEY = params.sessionKey;
  }
  if (params.senderJid) {
    env.OPENCLAW_CALLER_JID = params.senderJid;
  }
  if (params.accountId) {
    env.OPENCLAW_CALLER_ACCOUNT_ID = params.accountId;
  }

  return new Promise((resolve) => {
    const cwd = resolveGringoWorkspaceCwd();
    const execOptions: ExecFileOptions = {
      encoding: "utf8",
      env,
      maxBuffer: 256 * 1024,
      timeout: timeoutMs,
    };
    if (cwd) {
      execOptions.cwd = cwd;
    }

    execFile(
      ngrBin,
      [
        "coach",
        "context",
        "--phone",
        phone,
        "--surface",
        "dm",
        "--max-chars",
        process.env.OPENCLAW_GRINGO_COACH_CONTEXT_MAX_CHARS?.trim() || "3000",
        "--format",
        "json",
      ],
      execOptions,
      (error: ExecFileException | null, stdoutRaw: string | Buffer, stderrRaw: string | Buffer) => {
        const durationMs = Date.now() - startedAt;
        const output = stdoutRaw.toString().trim();
        if (output) {
          const status = isUnknownGringoUserContext(output) ? "unknown" : "success";
          const parsedContext = parseGringoCoachContextOutput(output);
          const prompt = parsedContext?.prompt ?? output;
          const promptForAgent = `${formatGringoPreloadedIdentityHint()}\n\n${prompt}`;
          if (sessionCacheKey) {
            setCachedGringoIdentityWorkingContext(
              sessionCacheKey,
              parsedContext?.workingContext ?? formatGringoCachedIdentityHint(),
            );
          }
          resolve({
            status,
            durationMs,
            bodyForAgentPrefix: promptForAgent,
            contextLength: promptForAgent.length,
            coachingContextLoaded: parsedContext?.coachingContextLoaded,
            coachingContextChars: parsedContext?.coachingContextChars,
            coachingContextVersion: parsedContext?.coachingContextVersion,
            userProfileContextLoaded: parsedContext?.userProfileContextLoaded,
            userProfileContextChars: parsedContext?.userProfileContextChars,
            trustedContextChars: parsedContext?.trustedContextChars,
            openClawSessionKeyForwarded: !!params.sessionKey,
          });
          return;
        }

        const message = error ? formatError(error) : stderrRaw.toString().trim();
        resolve({
          status: isProcessTimeoutError(error) ? "timeout" : "error",
          durationMs,
          error: elide(message || "identity preload returned no output", 240),
        });
      },
    );
  });
}

function readWhatsAppMessageReceivedHookOptIn(value: unknown): boolean | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const pluginHooks = (value as WhatsAppMessageReceivedHookConfig).pluginHooks;
  return pluginHooks?.messageReceived === true ? true : undefined;
}

function shouldEmitWhatsAppMessageReceivedHooks(params: {
  cfg: ReturnType<LoadConfigFn>;
  accountId?: string;
}): boolean {
  const channelConfig = params.cfg.channels?.whatsapp as
    | WhatsAppMessageReceivedHookConfig
    | undefined;
  const accountConfig =
    params.accountId && channelConfig?.accounts
      ? channelConfig.accounts[params.accountId]
      : undefined;
  return (
    readWhatsAppMessageReceivedHookOptIn(accountConfig) ??
    readWhatsAppMessageReceivedHookOptIn(channelConfig) ??
    false
  );
}

function emitWhatsAppMessageReceivedHooks(params: {
  ctx: ReturnType<typeof buildWhatsAppInboundContext>;
  sessionKey: string;
}): void {
  const canonical = deriveInboundMessageHookContext(params.ctx);
  const hookRunner = getGlobalHookRunner();
  if (hookRunner?.hasHooks("message_received")) {
    fireAndForgetBoundedHook(
      () =>
        hookRunner.runMessageReceived(
          toPluginMessageReceivedEvent(canonical),
          toPluginMessageContext(canonical),
        ),
      "whatsapp: message_received plugin hook failed",
      undefined,
      WHATSAPP_MESSAGE_RECEIVED_HOOK_LIMITS,
    );
  }
  fireAndForgetBoundedHook(
    () =>
      triggerInternalHook(
        createInternalHookEvent(
          "message",
          "received",
          params.sessionKey,
          toInternalMessageReceivedContext(canonical),
        ),
      ),
    "whatsapp: message_received internal hook failed",
    undefined,
    WHATSAPP_MESSAGE_RECEIVED_HOOK_LIMITS,
  );
}

function emitWhatsAppMessageReceivedHooksIfEnabled(params: {
  cfg: ReturnType<LoadConfigFn>;
  ctx: ReturnType<typeof buildWhatsAppInboundContext>;
  accountId?: string;
  sessionKey: string;
}): void {
  if (
    !shouldEmitWhatsAppMessageReceivedHooks({
      cfg: params.cfg,
      accountId: params.accountId,
    })
  ) {
    return;
  }

  emitWhatsAppMessageReceivedHooks({
    ctx: params.ctx,
    sessionKey: params.sessionKey,
  });
}

function resolvePinnedMainDmRecipient(params: {
  cfg: ReturnType<LoadConfigFn>;
  allowFrom?: string[];
}): string | null {
  return resolvePinnedMainDmOwnerFromAllowlist({
    dmScope: params.cfg.session?.dmScope,
    allowFrom: params.allowFrom,
    normalizeEntry: (entry) => normalizeE164(entry),
  });
}

export async function processMessage(params: {
  cfg: ReturnType<LoadConfigFn>;
  msg: WebInboundMsg;
  route: ReturnType<typeof resolveAgentRoute>;
  groupHistoryKey: string;
  groupHistories: Map<string, GroupHistoryEntry[]>;
  groupMemberNames: Map<string, Map<string, string>>;
  connectionId: string;
  verbose: boolean;
  maxMediaBytes: number;
  replyResolver: typeof getReplyFromConfig;
  replyLogger: ReturnType<typeof getChildLogger>;
  backgroundTasks: Set<Promise<unknown>>;
  rememberSentText: (
    text: string | undefined,
    opts: {
      combinedBody?: string;
      combinedBodySessionKey?: string;
      logVerboseMessage?: boolean;
    },
  ) => void;
  echoHas: (key: string) => boolean;
  echoForget: (key: string) => void;
  buildCombinedEchoKey: (p: { sessionKey: string; combinedBody: string }) => string;
  maxMediaTextChunkLimit?: number;
  groupHistory?: GroupHistoryEntry[];
  suppressGroupHistoryClear?: boolean;
  ackAlreadySent?: boolean;
  ackReaction?: AckReactionHandle | null;
  statusReactionController?: StatusReactionController | null;
  /** Pre-computed audio transcript from a caller-level preflight, used to avoid
   * re-transcribing the same voice note once per broadcast agent.
   * - string  → transcript obtained; use it directly, skip internal STT
   * - null    → preflight was attempted but failed / returned nothing; skip internal STT
   * - undefined (omitted) → caller did not attempt preflight; run internal STT as normal */
  preflightAudioTranscript?: string | null;
}) {
  const conversationId = params.msg.conversationId ?? params.msg.from;
  const self = getSelfIdentity(params.msg);
  const inboundPolicy = resolveWhatsAppInboundPolicy({
    cfg: params.cfg,
    accountId: params.route.accountId ?? params.msg.accountId,
    selfE164: self.e164 ?? null,
  });
  const account = inboundPolicy.account;
  const contextVisibilityMode = resolveChannelContextVisibilityMode({
    cfg: params.cfg,
    channel: "whatsapp",
    accountId: account.accountId,
  });
  const { storePath, envelopeOptions, previousTimestamp } = resolveInboundSessionEnvelopeContext({
    cfg: params.cfg,
    agentId: params.route.agentId,
    sessionKey: params.route.sessionKey,
  });
  // Preflight audio transcription: transcribe voice notes before building the
  // inbound context so the agent receives the transcript instead of <media:audio>.
  // Mirrors the preflight step added for Telegram in #61008.
  // When the caller already performed transcription (e.g. on-message.ts before
  // broadcast fan-out) the pre-computed result is reused to avoid N STT calls
  // for N broadcast agents on the same voice note.
  // preflightAudioTranscript semantics:
  //   string    → transcript ready, use it
  //   null      → caller attempted but got nothing; skip internal STT to avoid retry
  //   undefined → caller did not attempt; run internal STT
  let audioTranscript: string | undefined = params.preflightAudioTranscript ?? undefined;
  const hasAudioBody =
    params.msg.mediaType?.startsWith("audio/") === true && params.msg.body === "<media:audio>";
  if (params.preflightAudioTranscript === undefined && hasAudioBody && params.msg.mediaPath) {
    try {
      const { transcribeFirstAudio } = await import("./audio-preflight.runtime.js");
      audioTranscript = await transcribeFirstAudio({
        ctx: {
          MediaPaths: [params.msg.mediaPath],
          MediaTypes: params.msg.mediaType ? [params.msg.mediaType] : undefined,
          From: params.msg.from,
          To: params.msg.to,
          Provider: "whatsapp",
          Surface: "whatsapp",
          OriginatingChannel: "whatsapp",
          OriginatingTo: conversationId,
          AccountId: params.route.accountId,
        },
        cfg: params.cfg,
      });
    } catch {
      // Transcription failure is non-fatal: fall back to <media:audio> placeholder.
      if (shouldLogVerbose()) {
        logVerbose("whatsapp: audio preflight transcription failed, using placeholder");
      }
    }
  }

  // If we have a transcript, replace the agent-facing body so the agent sees the spoken text.
  // mediaPath and mediaType are intentionally preserved so that inboundAudio detection
  // (used by features such as messages.tts.auto: "inbound") still sees this as an
  // audio message. The transcript and transcribed media index are also stored on
  // context so downstream media understanding does not transcribe it again.
  const msgForAgent =
    audioTranscript !== undefined ? { ...params.msg, body: audioTranscript } : params.msg;

  let combinedBody = buildInboundLine({
    cfg: params.cfg,
    msg: msgForAgent,
    agentId: params.route.agentId,
    previousTimestamp,
    envelope: envelopeOptions,
  });
  let shouldClearGroupHistory = false;
  const visibleGroupHistory =
    params.msg.chatType === "group"
      ? resolveVisibleWhatsAppGroupHistory({
          history: params.groupHistory ?? params.groupHistories.get(params.groupHistoryKey) ?? [],
          mode: contextVisibilityMode,
          groupPolicy: inboundPolicy.groupPolicy,
          groupAllowFrom: inboundPolicy.groupAllowFrom,
        })
      : undefined;

  if (params.msg.chatType === "group") {
    const history = visibleGroupHistory ?? [];
    if (history.length > 0) {
      const historyEntries: HistoryEntry[] = history.map((m) => ({
        sender: m.sender,
        body: m.body,
        timestamp: m.timestamp,
      }));
      combinedBody = buildHistoryContextFromEntries({
        entries: historyEntries,
        currentMessage: combinedBody,
        excludeLast: false,
        formatEntry: (entry) => {
          return formatInboundEnvelope({
            channel: "WhatsApp",
            from: conversationId,
            timestamp: entry.timestamp,
            body: entry.body,
            chatType: "group",
            senderLabel: entry.sender,
            envelope: envelopeOptions,
          });
        },
      });
    }
    shouldClearGroupHistory = !(params.suppressGroupHistoryClear ?? false);
  }

  // Echo detection uses combined body so we don't respond twice.
  const combinedEchoKey = params.buildCombinedEchoKey({
    sessionKey: params.route.sessionKey,
    combinedBody,
  });
  if (params.echoHas(combinedEchoKey)) {
    logVerbose("Skipping auto-reply: detected echo for combined message");
    params.echoForget(combinedEchoKey);
    return false;
  }

  // When statusReactions.enabled, a StatusReactionController takes over lifecycle
  // signaling (queued → thinking → tool → done/error). The plain ackReaction is
  // skipped so the same message slot isn't used for two competing systems.
  const statusReactionController =
    params.statusReactionController ??
    (params.cfg.messages?.statusReactions?.enabled === true && !params.ackAlreadySent
      ? await createWhatsAppStatusReactionController({
          cfg: params.cfg,
          msg: params.msg,
          agentId: params.route.agentId,
          sessionKey: params.route.sessionKey,
          conversationId,
          verbose: params.verbose,
          accountId: account.accountId,
        })
      : null);

  if (statusReactionController && !params.statusReactionController) {
    void statusReactionController.setQueued();
  }

  // Send ack reaction immediately upon message receipt (post-gating). Callers
  // that do preflight work before processMessage can send it first and set
  // ackAlreadySent so slow STT does not delay user-visible receipt feedback.
  // Skip if the status reaction controller is handling lifecycle signaling.
  let ackReaction = params.ackReaction ?? null;
  if (!statusReactionController && !ackReaction && params.ackAlreadySent !== true) {
    ackReaction = await maybeSendAckReaction({
      cfg: params.cfg,
      msg: params.msg,
      agentId: params.route.agentId,
      sessionKey: params.route.sessionKey,
      conversationId,
      verbose: params.verbose,
      accountId: account.accountId,
      info: params.replyLogger.info.bind(params.replyLogger),
      warn: params.replyLogger.warn.bind(params.replyLogger),
    });
  }

  const correlationId = params.msg.id ?? newConnectionId();
  params.replyLogger.info(
    {
      connectionId: params.connectionId,
      correlationId,
      from: params.msg.chatType === "group" ? conversationId : params.msg.from,
      to: params.msg.to,
      body: elide(combinedBody, 240),
      mediaType: params.msg.mediaType ?? null,
      mediaPath: params.msg.mediaPath ?? null,
    },
    "inbound web message",
  );

  const fromDisplay = params.msg.chatType === "group" ? conversationId : params.msg.from;
  const kindLabel = params.msg.mediaType ? `, ${params.msg.mediaType}` : "";
  whatsappInboundLog.info(
    `Inbound message ${fromDisplay} -> ${params.msg.to} (${params.msg.chatType}${kindLabel}, ${combinedBody.length} chars)`,
  );
  if (shouldLogVerbose()) {
    whatsappInboundLog.debug(`Inbound body: ${elide(combinedBody, 400)}`);
  }

  const sender = getSenderIdentity(params.msg);
  const identityPreload = await preloadGringoIdentityContext({
    agentId: params.route.agentId,
    chatType: params.msg.chatType,
    sessionKey: params.route.sessionKey,
    correlationId,
    inboundText: combinedBody,
    refreshText: params.msg.body,
    phone: sender.e164 ?? params.msg.senderE164,
    senderJid: params.msg.senderJid,
    accountId: params.route.accountId ?? params.msg.accountId,
  });
  if (params.route.agentId === GRINGO_AGENT_ID && identityPreload.status !== "skipped") {
    params.replyLogger.info(
      {
        accountId: params.route.accountId ?? params.msg.accountId,
        agentId: params.route.agentId,
        correlationId,
        chatType: params.msg.chatType,
        identityPreloadStatus: identityPreload.status,
        identityPreloadDurationMs: identityPreload.durationMs,
        identityPreloadContextLength: identityPreload.contextLength ?? null,
        coachingContextLoaded: identityPreload.coachingContextLoaded ?? null,
        coachingContextChars: identityPreload.coachingContextChars ?? null,
        coachingContextVersion: identityPreload.coachingContextVersion ?? null,
        userProfileContextLoaded: identityPreload.userProfileContextLoaded ?? null,
        userProfileContextChars: identityPreload.userProfileContextChars ?? null,
        trustedContextChars: identityPreload.trustedContextChars ?? null,
        openClawSessionKeyForwarded: identityPreload.openClawSessionKeyForwarded ?? null,
        identityPreloadError: identityPreload.error ?? null,
      },
      "gringo identity preload completed",
    );
  }
  const bodyForAgent = identityPreload.bodyForAgentPrefix
    ? `${identityPreload.bodyForAgentPrefix}\n\nUser message:\n${msgForAgent.body}`
    : msgForAgent.body;
  const visibleReplyTo = resolveVisibleWhatsAppReplyContext({
    msg: params.msg,
    authDir: account.authDir,
    mode: contextVisibilityMode,
    groupPolicy: inboundPolicy.groupPolicy,
    groupAllowFrom: inboundPolicy.groupAllowFrom,
  });
  const dmRouteTarget = resolveWhatsAppDmRouteTarget({
    msg: params.msg,
    senderE164: sender.e164 ?? undefined,
    normalizeE164,
  });
  const shouldCheckCommandAuth = shouldComputeCommandAuthorized(params.msg.body, params.cfg);
  const isTextCommand = isControlCommandMessage(params.msg.body, params.cfg);
  const commandAuthorized = shouldCheckCommandAuth
    ? await resolveWhatsAppCommandAuthorized({
        cfg: params.cfg,
        msg: params.msg,
        policy: inboundPolicy,
      })
    : undefined;
  const commandTurn: CommandTurnContext = isTextCommand
    ? {
        kind: "text-slash",
        source: "text",
        authorized: Boolean(commandAuthorized),
        body: params.msg.body,
      }
    : {
        kind: "normal",
        source: "message",
        authorized: false,
        body: params.msg.body,
      };
  const { onModelSelected, ...replyPipeline } = createChannelMessageReplyPipeline({
    cfg: params.cfg,
    agentId: params.route.agentId,
    channel: "whatsapp",
    accountId: params.route.accountId,
  });
  const responsePrefix = resolveWhatsAppResponsePrefix({
    cfg: params.cfg,
    agentId: params.route.agentId,
    isSelfChat: params.msg.chatType !== "group" && inboundPolicy.isSelfChat,
    pipelineResponsePrefix: replyPipeline.responsePrefix,
  });
  const replyThreading = resolveBatchedReplyThreadingPolicy(
    account.replyToMode ?? "off",
    params.msg.isBatched === true,
  );

  // Resolve combined conversation system prompt using the group or direct surface.
  const conversationSystemPrompt =
    params.msg.chatType === "group"
      ? resolveWhatsAppGroupSystemPrompt({
          accountConfig: account,
          groupId: conversationId,
        })
      : resolveWhatsAppDirectSystemPrompt({
          accountConfig: account,
          peerId: dmRouteTarget ?? params.msg.from,
        });

  const ctxPayload = buildWhatsAppInboundContext({
    bodyForAgent,
    combinedBody,
    commandBody: params.msg.body,
    commandAuthorized,
    commandTurn,
    conversationId,
    groupHistory: visibleGroupHistory,
    groupMemberRoster: params.groupMemberNames.get(params.groupHistoryKey),
    groupSystemPrompt: conversationSystemPrompt,
    msg: params.msg,
    rawBody: params.msg.body,
    route: params.route,
    sender: {
      id: getPrimaryIdentityId(sender) ?? undefined,
      name: sender.name ?? undefined,
      e164: sender.e164 ?? undefined,
    },
    ...(audioTranscript !== undefined ? { transcript: audioTranscript } : {}),
    ...(audioTranscript !== undefined ? { mediaTranscribedIndexes: [0] } : {}),
    replyThreading,
    visibleReplyTo: visibleReplyTo ?? undefined,
  });
  emitWhatsAppMessageReceivedHooksIfEnabled({
    cfg: params.cfg,
    ctx: ctxPayload,
    accountId: params.route.accountId,
    sessionKey: params.route.sessionKey,
  });

  const pinnedMainDmRecipient = resolvePinnedMainDmRecipient({
    cfg: params.cfg,
    allowFrom: inboundPolicy.configuredAllowFrom,
  });
  updateWhatsAppMainLastRoute({
    backgroundTasks: params.backgroundTasks,
    cfg: params.cfg,
    ctx: ctxPayload,
    dmRouteTarget,
    pinnedMainDmRecipient,
    route: params.route,
    updateLastRoute: updateLastRouteInBackground,
    warn: params.replyLogger.warn.bind(params.replyLogger),
  });

  const turnResult = await runInboundReplyTurn({
    channel: "whatsapp",
    accountId: params.route.accountId,
    raw: params.msg,
    adapter: {
      ingest: () => ({
        id: params.msg.id ?? `${conversationId}:${Date.now()}`,
        timestamp: params.msg.timestamp,
        rawText: ctxPayload.RawBody ?? "",
        textForAgent: ctxPayload.BodyForAgent,
        textForCommands: ctxPayload.CommandBody,
        raw: params.msg,
      }),
      resolveTurn: () => ({
        channel: "whatsapp",
        accountId: params.route.accountId,
        routeSessionKey: params.route.sessionKey,
        storePath,
        ctxPayload,
        recordInboundSession,
        record: {
          onRecordError: (err) => {
            params.replyLogger.warn(
              {
                error: formatError(err),
                storePath,
                sessionKey: params.route.sessionKey,
              },
              "failed updating session meta",
            );
          },
          trackSessionMetaTask: (task) => {
            trackBackgroundTask(params.backgroundTasks, task);
          },
        },
        runDispatch: () =>
          dispatchWhatsAppBufferedReply({
            cfg: params.cfg,
            connectionId: params.connectionId,
            context: ctxPayload,
            conversationId,
            deliverReply: deliverWebReply,
            groupHistories: params.groupHistories,
            groupHistoryKey: params.groupHistoryKey,
            maxMediaBytes: params.maxMediaBytes,
            maxMediaTextChunkLimit: params.maxMediaTextChunkLimit,
            msg: params.msg,
            onModelSelected,
            rememberSentText: params.rememberSentText,
            replyLogger: params.replyLogger,
            replyPipeline: {
              ...replyPipeline,
              responsePrefix,
            },
            replyResolver: params.replyResolver,
            route: params.route,
            shouldClearGroupHistory,
            statusReactionController,
          }),
      }),
    },
  });
  const didSendReply = turnResult.dispatched ? turnResult.dispatchResult : false;
  removeAckReactionHandleAfterReply({
    removeAfterReply: Boolean(params.cfg.messages?.removeAckAfterReply && didSendReply),
    ackReaction,
    onError: (err) => {
      logAckFailure({
        log: logVerbose,
        channel: "whatsapp",
        target: `${params.msg.chatId ?? conversationId}/${params.msg.id ?? "unknown"}`,
        error: err,
      });
    },
  });
  return didSendReply;
}
