import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WhatsAppSendResult } from "../../inbound/send-result.js";

// Hoisted mocks used across tests so vi.mock factories can reference them.
const {
  resolvePolicyMock,
  buildContextMock,
  isControlCommandMessageMock,
  runMessageReceivedMock,
  shouldComputeCommandAuthorizedMock,
  trackBackgroundTaskMock,
  deliverWebReplyMock,
  execFileMock,
} = vi.hoisted(() => ({
  resolvePolicyMock: vi.fn(),
  buildContextMock: vi.fn(),
  isControlCommandMessageMock: vi.fn(() => false),
  runMessageReceivedMock: vi.fn(async () => undefined),
  shouldComputeCommandAuthorizedMock: vi.fn(() => false),
  trackBackgroundTaskMock: vi.fn(),
  deliverWebReplyMock: vi.fn(async () => ({
    results: [],
    receipt: { platformMessageIds: ["upsell-1"], parts: [] },
    providerAccepted: true,
  })),
  execFileMock: vi.fn(),
}));

function acceptedSendResult(kind: "media" | "text", id: string): WhatsAppSendResult {
  return {
    kind,
    messageId: id,
    keys: [{ id }],
    providerAccepted: true,
  };
}

vi.mock("../../inbound-policy.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../inbound-policy.js")>();
  return {
    ...actual,
    resolveWhatsAppCommandAuthorized: async () => true,
    resolveWhatsAppInboundPolicy: resolvePolicyMock,
  };
});

vi.mock("./inbound-dispatch.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./inbound-dispatch.js")>();
  return {
    ...actual,
    buildWhatsAppInboundContext: buildContextMock,
    dispatchWhatsAppBufferedReply: async () => ({
      queuedFinal: false,
      counts: { tool: 0, block: 0, final: 0 },
    }),
    resolveWhatsAppDmRouteTarget: () => null,
    resolveWhatsAppResponsePrefix: () => undefined,
    updateWhatsAppMainLastRoute: () => {},
  };
});

vi.mock("openclaw/plugin-sdk/plugin-runtime", () => ({
  getGlobalHookRunner: () => ({
    hasHooks: (hookName: string) => hookName === "message_received",
    runMessageReceived: runMessageReceivedMock,
  }),
}));

vi.mock("../../identity.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../identity.js")>();
  return {
    ...actual,
    getPrimaryIdentityId: () => null,
    getSelfIdentity: () => ({ e164: "+15550001111" }),
    getSenderIdentity: () => ({ name: "Alice", e164: "+15550002222" }),
  };
});

vi.mock("../../reconnect.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../reconnect.js")>();
  return { ...actual, newConnectionId: () => "test-conn-id" };
});

vi.mock("../../session.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../session.js")>();
  return { ...actual, formatError: (e: unknown) => String(e) };
});

vi.mock("../deliver-reply.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../deliver-reply.js")>();
  return { ...actual, deliverWebReply: deliverWebReplyMock };
});

vi.mock("../loggers.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../loggers.js")>();
  return {
    ...actual,
    whatsappInboundLog: { info: () => {}, debug: () => {} },
  };
});

vi.mock("./ack-reaction.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./ack-reaction.js")>();
  return { ...actual, maybeSendAckReaction: async () => {} };
});

vi.mock("./inbound-context.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./inbound-context.js")>();
  return {
    ...actual,
    resolveVisibleWhatsAppGroupHistory: () => [],
    resolveVisibleWhatsAppReplyContext: () => null,
  };
});

vi.mock("./last-route.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./last-route.js")>();
  return {
    ...actual,
    trackBackgroundTask: trackBackgroundTaskMock,
    updateLastRouteInBackground: () => {},
  };
});

vi.mock("./message-line.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./message-line.js")>();
  return { ...actual, buildInboundLine: () => "hi" };
});

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return {
    ...actual,
    execFile: execFileMock,
  };
});

vi.mock("./runtime-api.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./runtime-api.js")>();
  return {
    ...actual,
    buildHistoryContextFromEntries: () => "hi",
    createChannelMessageReplyPipeline: () => ({
      onModelSelected: () => {},
      responsePrefix: undefined,
    }),
    formatInboundEnvelope: () => "hi",
    logVerbose: () => {},
    normalizeE164: (v: string) => v,
    recordSessionMetaFromInbound: async () => {},
    resolveChannelContextVisibilityMode: () => "off",
    resolveInboundSessionEnvelopeContext: () => ({
      storePath: "/tmp",
      envelopeOptions: {},
      previousTimestamp: undefined,
    }),
    resolvePinnedMainDmOwnerFromAllowlist: () => null,
    isControlCommandMessage: isControlCommandMessageMock,
    shouldComputeCommandAuthorized: shouldComputeCommandAuthorizedMock,
    shouldLogVerbose: () => false,
  };
});

import { clearInternalHooks, registerInternalHook } from "openclaw/plugin-sdk/hook-runtime";
import {
  clearGringoIdentityPreloadSessionCacheForTests,
  processMessage,
} from "./process-message.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeAccount(groups: Record<string, { systemPrompt?: string }> = {}): {
  accountId: string;
  authDir: string;
  groups: Record<string, { systemPrompt?: string }>;
} {
  return { accountId: "default", authDir: "/tmp/wa-test-auth", groups };
}

function makePolicy(account: ReturnType<typeof makeAccount>) {
  return {
    account,
    dmPolicy: "pairing",
    groupPolicy: "allowlist",
    configuredAllowFrom: [],
    dmAllowFrom: [],
    groupAllowFrom: [],
    isSelfChat: false,
    providerMissingFallbackApplied: false,
    isSamePhone: () => false,
    resolveConversationGroupPolicy: () => "allowlist",
    resolveConversationRequireMention: () => false,
  };
}

const GROUP_JID = "123@g.us";

const baseMsg = {
  id: "msg1",
  from: GROUP_JID,
  to: "+15550001111",
  conversationId: GROUP_JID,
  accountId: "default",
  chatId: GROUP_JID,
  chatType: "group" as const,
  body: "hi",
  sendComposing: async () => {},
  reply: async () => acceptedSendResult("text", "r1"),
  sendMedia: async () => acceptedSendResult("media", "m1"),
};

const baseRoute = {
  agentId: "main",
  channel: "whatsapp",
  accountId: "default",
  sessionKey: "agent:main:whatsapp:group:123@g.us",
  mainSessionKey: "agent:main:whatsapp:group:123@g.us",
  lastRoutePolicy: "main",
  matchedBy: "default",
};

function callProcessMessage(
  overrides: { cfg?: unknown; msg?: unknown; route?: unknown; replyLogger?: unknown } = {},
) {
  return processMessage({
    cfg: (overrides.cfg ?? {}) as never,
    msg: (overrides.msg ?? baseMsg) as never,
    route: (overrides.route ?? baseRoute) as never,
    groupHistoryKey: "whatsapp:default:group:123@g.us",
    groupHistories: new Map(),
    groupMemberNames: new Map(),
    connectionId: "conn-1",
    verbose: false,
    maxMediaBytes: 1024,
    replyResolver: (async () => undefined) as never,
    replyLogger: (overrides.replyLogger ?? {
      info: () => {},
      warn: () => {},
      error: () => {},
      debug: () => {},
    }) as never,
    backgroundTasks: new Set(),
    rememberSentText: () => {},
    echoHas: () => false,
    echoForget: () => {},
    buildCombinedEchoKey: ({ sessionKey }) => sessionKey,
  });
}

function mockCallArg(mockFn: ReturnType<typeof vi.fn>, label: string, callIndex = 0, argIndex = 0) {
  const call = mockFn.mock.calls.at(callIndex);
  if (!call) {
    throw new Error(`Expected ${label} call ${callIndex}`);
  }
  if (!(argIndex in call)) {
    throw new Error(`Expected ${label} call ${callIndex} argument ${argIndex}`);
  }
  return call[argIndex];
}

function trustedContextAt(callIndex: number): string {
  const params = mockCallArg(buildContextMock, "buildWhatsAppInboundContext", callIndex) as {
    trustedContext?: string[];
  };
  return params.trustedContext?.join("\n") ?? "";
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("processMessage group system prompt wiring", () => {
  beforeEach(() => {
    buildContextMock.mockReset();
    isControlCommandMessageMock.mockReset();
    isControlCommandMessageMock.mockReturnValue(false);
    resolvePolicyMock.mockReset();
    runMessageReceivedMock.mockClear();
    shouldComputeCommandAuthorizedMock.mockReset();
    shouldComputeCommandAuthorizedMock.mockReturnValue(false);
    trackBackgroundTaskMock.mockClear();
    deliverWebReplyMock.mockClear();
    execFileMock.mockReset();
    clearGringoIdentityPreloadSessionCacheForTests();
    delete process.env.OPENCLAW_GRINGO_IDENTITY_PRELOAD;
    delete process.env.OPENCLAW_GRINGO_IDENTITY_PRELOAD_EVERY_TURN;
    clearInternalHooks();
    buildContextMock.mockImplementation(
      (params: { groupSystemPrompt?: string; combinedBody?: string }) => ({
        GroupSystemPrompt: params.groupSystemPrompt,
        Body: params.combinedBody ?? "",
      }),
    );
  });

  afterEach(() => {
    vi.useRealTimers();
    clearInternalHooks();
  });

  it("resolves group systemPrompt from account config and passes it into buildWhatsAppInboundContext", async () => {
    resolvePolicyMock.mockReturnValue(
      makePolicy(makeAccount({ [GROUP_JID]: { systemPrompt: "from config" } })),
    );

    await callProcessMessage();

    expect(
      (
        mockCallArg(buildContextMock, "buildWhatsAppInboundContext") as {
          groupSystemPrompt?: string;
        }
      ).groupSystemPrompt,
    ).toBe("from config");
  });

  it("marks detected WhatsApp slash messages as text command turns", async () => {
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    isControlCommandMessageMock.mockReturnValue(true);
    shouldComputeCommandAuthorizedMock.mockReturnValue(true);

    await callProcessMessage({
      msg: {
        ...baseMsg,
        body: "/status",
      },
    });

    expect(shouldComputeCommandAuthorizedMock).toHaveBeenCalledWith("/status", {});
    expect(isControlCommandMessageMock).toHaveBeenCalledWith("/status", {});
    expect(buildContextMock.mock.calls[0][0]).toMatchObject({
      commandBody: "/status",
      commandAuthorized: true,
      commandTurn: {
        kind: "text-slash",
        source: "text",
        authorized: true,
        body: "/status",
      },
      rawBody: "/status",
    });
  });

  it("checks auth for inline command tokens without marking them as command-source turns", async () => {
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    isControlCommandMessageMock.mockReturnValue(false);
    shouldComputeCommandAuthorizedMock.mockReturnValue(true);

    await callProcessMessage({
      msg: {
        ...baseMsg,
        body: "please inspect `/tmp/foo`",
      },
    });

    expect(buildContextMock.mock.calls[0][0]).toMatchObject({
      commandBody: "please inspect `/tmp/foo`",
      commandAuthorized: true,
      commandTurn: {
        kind: "normal",
        source: "message",
        authorized: false,
        body: "please inspect `/tmp/foo`",
      },
      rawBody: "please inspect `/tmp/foo`",
    });
    expect(buildContextMock.mock.calls[0][0].commandSource).toBeUndefined();
  });

  it("fires message_received hooks with canonical WhatsApp correlation fields", async () => {
    const internalReceived = vi.fn();
    registerInternalHook("message:received", internalReceived);
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    buildContextMock.mockImplementationOnce(() => ({
      Body: "hi",
      BodyForCommands: "hi",
      RawBody: "hi",
      CommandBody: "hi",
      From: GROUP_JID,
      To: "+15550001111",
      SessionKey: baseRoute.sessionKey,
      AccountId: "default",
      MessageSid: "msg1",
      SenderId: "+15550002222",
      SenderName: "Alice",
      SenderE164: "+15550002222",
      Timestamp: 1710000000,
      Provider: "whatsapp",
      Surface: "whatsapp",
      OriginatingChannel: "whatsapp",
      OriginatingTo: GROUP_JID,
      GroupSubject: "Test Group",
    }));

    await callProcessMessage({
      cfg: {
        channels: {
          whatsapp: {
            pluginHooks: {
              messageReceived: true,
            },
          },
        },
      },
    });
    await Promise.resolve();
    await Promise.resolve();

    expect(runMessageReceivedMock).toHaveBeenCalledTimes(1);
    expect(runMessageReceivedMock).toHaveBeenCalledWith(
      {
        from: GROUP_JID,
        content: "hi",
        timestamp: 1710000000,
        threadId: undefined,
        messageId: "msg1",
        senderId: "+15550002222",
        sessionKey: baseRoute.sessionKey,
        runId: undefined,
        metadata: {
          to: "+15550001111",
          provider: "whatsapp",
          surface: "whatsapp",
          threadId: undefined,
          originatingChannel: "whatsapp",
          originatingTo: GROUP_JID,
          messageId: "msg1",
          senderId: "+15550002222",
          senderName: "Alice",
          senderUsername: undefined,
          senderE164: "+15550002222",
          guildId: undefined,
          channelName: undefined,
          topicName: undefined,
        },
      },
      {
        channelId: "whatsapp",
        accountId: "default",
        conversationId: GROUP_JID,
        sessionKey: baseRoute.sessionKey,
        messageId: "msg1",
        senderId: "+15550002222",
      },
    );
    expect(internalReceived).toHaveBeenCalledTimes(1);
    const internalEvent = mockCallArg(internalReceived, "internal message received") as Record<
      string,
      unknown
    >;
    expect(internalEvent.timestamp).toBeInstanceOf(Date);
    expect({ ...internalEvent, timestamp: undefined }).toEqual({
      type: "message",
      action: "received",
      sessionKey: baseRoute.sessionKey,
      context: {
        from: GROUP_JID,
        content: "hi",
        timestamp: 1710000000,
        channelId: "whatsapp",
        accountId: "default",
        conversationId: GROUP_JID,
        messageId: "msg1",
        metadata: {
          to: "+15550001111",
          provider: "whatsapp",
          surface: "whatsapp",
          threadId: undefined,
          senderId: "+15550002222",
          senderName: "Alice",
          senderUsername: undefined,
          senderE164: "+15550002222",
          guildId: undefined,
          channelName: undefined,
          topicName: undefined,
        },
      },
      timestamp: undefined,
      messages: [],
    });
  });

  it("does not fire WhatsApp message_received hooks without explicit opt-in", async () => {
    const internalReceived = vi.fn();
    registerInternalHook("message:received", internalReceived);
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));

    await callProcessMessage();

    expect(runMessageReceivedMock).not.toHaveBeenCalled();
    expect(internalReceived).not.toHaveBeenCalled();
  });

  it("preloads Gringo identity context into the agent-facing body", async () => {
    const replyInfo = vi.fn();
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    execFileMock.mockImplementation(
      (
        _bin: string,
        _args: string[],
        _options: unknown,
        callback: (error: null, stdout: string, stderr: string) => void,
      ) => {
        callback(
          null,
          JSON.stringify({
            ok: true,
            data: {
              prompt:
                "Trusted NaGringa coaching context\n\n# Alice - paid yearly\n\n- **Access:** paid yearly · DM: enabled\n\n## Coaching State Memory\n\nActive loop: Nubank",
              workingContext:
                "Trusted NaGringa working context\n- User: Alice · paid yearly · DM: enabled\n- Role: Senior Product Engineer\n- Coaching memory: loaded\n- Use this as the trusted identity/context card. Refresh only if asked or after profile/memory updates.",
              coachingState: {
                loaded: true,
                included: 19,
                updatedAt: "2026-05-06T00:00:00Z",
              },
              userProfile: {
                loaded: true,
                included: 8,
              },
              access: {
                label: "paid yearly · DM: enabled",
                hasAccess: true,
                dmEnabled: true,
                isAdmin: false,
                accessModel: "subscription",
                cadence: "yearly",
              },
              quota: {
                allowed: true,
                limit: 10,
                used: 4,
                remaining: 6,
                monthKey: "2026-05",
                accessTier: "paid_subscription",
              },
            },
          }),
          "",
        );
      },
    );
    buildContextMock.mockImplementationOnce((params: { bodyForAgent?: string }) => ({
      Body: "hi",
      BodyForAgent: params.bodyForAgent,
      RawBody: "hi",
      CommandBody: "hi",
      SessionKey: "agent:gringo:whatsapp:direct:+15550002222",
      Provider: "whatsapp",
      Surface: "whatsapp",
    }));

    await callProcessMessage({
      msg: {
        ...baseMsg,
        from: "+15550002222",
        conversationId: "+15550002222",
        chatId: "+15550002222",
        chatType: "direct",
        senderE164: "+15550002222",
        senderJid: "15550002222@s.whatsapp.net",
      },
      route: {
        ...baseRoute,
        agentId: "gringo",
        sessionKey: "agent:gringo:whatsapp:direct:+15550002222",
        mainSessionKey: "agent:gringo:whatsapp:direct:+15550002222",
      },
      replyLogger: {
        info: replyInfo,
        warn: () => {},
        error: () => {},
        debug: () => {},
      },
    });

    expect(execFileMock).toHaveBeenCalledWith(
      "ngr",
      [
        "coach",
        "context",
        "--phone",
        "+15550002222",
        "--surface",
        "dm",
        "--max-chars",
        "3000",
        "--format",
        "json",
      ],
      expect.objectContaining({
        env: expect.objectContaining({
          OPENCLAW_CALLER_CHANNEL: "whatsapp",
          OPENCLAW_CALLER_PHONE: "+15550002222",
          OPENCLAW_CALLER_JID: "15550002222@s.whatsapp.net",
          OPENCLAW_AGENT_ID: "gringo",
          OPENCLAW_CHAT_TYPE: "direct",
          OPENCLAW_SESSION_KEY: "agent:gringo:whatsapp:direct:+15550002222",
          OPENCLAW_CORRELATION_ID: "msg1",
          OPENCLAW_TURN_ID: "msg1",
          OPENCLAW_INBOUND_TEXT: "hi",
        }),
        timeout: 2500,
      }),
      expect.any(Function),
    );
    expect(buildContextMock.mock.calls[0][0].bodyForAgent).toBe("hi");
    expect(trustedContextAt(0)).toContain("Trusted context for this turn is preloaded.");
    expect(trustedContextAt(0)).toContain("Trusted NaGringa coaching context");
    expect(trustedContextAt(0)).toContain("Active loop: Nubank");
    expect(trustedContextAt(0)).not.toContain("User message:\nhi");
    expect(replyInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        agentId: "gringo",
        identityPreloadStatus: "success",
        identityPreloadContextLength: expect.any(Number),
        gringoAccessTier: "paid_subscription",
        gringoAccessModel: "subscription",
        gringoAccessHasAccess: true,
        gringoAccessDmEnabled: true,
        gringoAccessIsAdmin: false,
        gringoAccessCadence: "yearly",
        coachingContextLoaded: true,
        coachingContextChars: 19,
        coachingContextVersion: "2026-05-06T00:00:00Z",
        userProfileContextLoaded: true,
        userProfileContextChars: 8,
        trustedContextChars: expect.any(Number),
        openClawSessionKeyForwarded: true,
        gringoQuotaAllowed: true,
        gringoQuotaLimit: 10,
        gringoQuotaUsed: 4,
        gringoQuotaRemaining: 6,
        gringoQuotaMonthKey: "2026-05",
        gringoQuotaAccessTier: "paid_subscription",
        gringoQuotaCheckDurationMs: null,
        gringoQuotaSource: "context",
      }),
      "gringo identity preload completed",
    );
  });

  it("injects only a compact trusted sender hint into group messages", async () => {
    const replyInfo = vi.fn();
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    await callProcessMessage({
      msg: {
        ...baseMsg,
        chatType: "group",
        senderE164: "+15550002222",
        senderJid: "15550002222@s.whatsapp.net",
      },
      route: {
        ...baseRoute,
        agentId: "gringo",
        sessionKey: "agent:gringo:whatsapp:group:123@g.us",
        mainSessionKey: "agent:gringo:whatsapp:group:123@g.us",
      },
      replyLogger: {
        info: replyInfo,
        warn: () => {},
        error: () => {},
        debug: () => {},
      },
    });

    expect(execFileMock).not.toHaveBeenCalled();
    expect(buildContextMock.mock.calls[0][0].bodyForAgent).toBe("hi");
    expect(trustedContextAt(0)).toContain("Trusted WhatsApp group sender: +15550002222");
    expect(trustedContextAt(0)).toContain(
      "ngr coach context --phone <trusted_phone> --surface group",
    );
    expect(trustedContextAt(0)).not.toContain("User message:\nhi");
    expect(trustedContextAt(0)).not.toContain("Trusted NaGringa coaching context");
    expect(replyInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        agentId: "gringo",
        chatType: "group",
        identityPreloadStatus: "group_hint",
        identityPreloadContextLength: expect.any(Number),
      }),
      "gringo identity preload completed",
    );
  });

  it("sends the Gringo quota upsell without running the agent when free limit is reached", async () => {
    const replyInfo = vi.fn();
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    execFileMock.mockImplementation(
      (
        _bin: string,
        _args: string[],
        _options: unknown,
        callback: (error: null, stdout: string, stderr: string) => void,
      ) => {
        callback(
          null,
          JSON.stringify({
            ok: true,
            data: {
              prompt:
                "Trusted NaGringa coaching context\n\n# Alice - free\n\n- **Access:** free tier · DM: gated",
              workingContext: "Trusted NaGringa working context\n- User: Alice · free",
              access: {
                label: "free tier · DM: gated",
                hasAccess: false,
                dmEnabled: false,
                isAdmin: false,
              },
              quota: {
                allowed: false,
                limit: 10,
                used: 10,
                remaining: 0,
                monthKey: "2026-05",
                accessTier: "free",
                blockedReason: "free_monthly_limit",
              },
            },
          }),
          "",
        );
      },
    );

    const sent = await callProcessMessage({
      msg: {
        ...baseMsg,
        from: "+15550006666",
        conversationId: "+15550006666",
        chatId: "+15550006666",
        chatType: "direct",
        senderE164: "+15550006666",
        senderJid: "15550006666@s.whatsapp.net",
      },
      route: {
        ...baseRoute,
        agentId: "gringo",
        sessionKey: "agent:gringo:whatsapp:direct:+15550006666",
        mainSessionKey: "agent:gringo:whatsapp:direct:+15550006666",
      },
      replyLogger: {
        info: replyInfo,
        warn: () => {},
        error: () => {},
        debug: () => {},
      },
    });

    expect(sent).toBe(true);
    expect(buildContextMock).not.toHaveBeenCalled();
    expect(deliverWebReplyMock).toHaveBeenCalledWith(
      expect.objectContaining({
        replyResult: {
          text: expect.stringContaining("limite gratuito de 10 mensagens"),
        },
      }),
    );
    expect(replyInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        gringoQuotaAllowed: false,
        gringoQuotaBlockedReason: "free_monthly_limit",
        providerAccepted: true,
      }),
      "gringo quota upsell sent",
    );
  });

  it("sends the Gringo account-required message without running the agent for unknown WhatsApp phones", async () => {
    const replyInfo = vi.fn();
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    execFileMock.mockImplementation(
      (
        _bin: string,
        _args: string[],
        _options: unknown,
        callback: (error: null, stdout: string, stderr: string) => void,
      ) => {
        callback(
          null,
          JSON.stringify({
            ok: true,
            data: {
              identity: {
                resolved: true,
                phone: "+15550007777",
                source: "transport_phone",
              },
              prompt:
                "Trusted NaGringa coaching context\n\n# Unknown user - +15550007777\n\n- **Missing fields:** appAccount\n\n## Account Required",
              workingContext:
                "Trusted NaGringa working context\n- User: Unknown user\n- Missing fields: appAccount\n- Account: not linked to a NaGringa app account",
              missingFields: ["appAccount"],
              access: {
                label: "free tier · DM: gated",
                hasAccess: false,
                dmEnabled: false,
                isAdmin: false,
              },
              quota: {
                allowed: true,
                limit: 10,
                used: 1,
                remaining: 9,
                monthKey: "2026-05",
                accessTier: "free",
              },
            },
          }),
          "",
        );
      },
    );

    const sent = await callProcessMessage({
      msg: {
        ...baseMsg,
        from: "+15550007777",
        conversationId: "+15550007777",
        chatId: "+15550007777",
        chatType: "direct",
        senderE164: "+15550007777",
        senderJid: "15550007777@s.whatsapp.net",
      },
      route: {
        ...baseRoute,
        agentId: "gringo",
        sessionKey: "agent:gringo:whatsapp:direct:+15550007777",
        mainSessionKey: "agent:gringo:whatsapp:direct:+15550007777",
      },
      replyLogger: {
        info: replyInfo,
        warn: () => {},
        error: () => {},
        debug: () => {},
      },
    });

    expect(sent).toBe(true);
    expect(buildContextMock).not.toHaveBeenCalled();
    expect(deliverWebReplyMock).toHaveBeenCalledWith(
      expect.objectContaining({
        replyResult: {
          text: expect.stringContaining("conta NaGringa ligada a este WhatsApp"),
        },
      }),
    );
    expect(replyInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        identityPreloadStatus: "unknown",
        gringoQuotaAllowed: true,
      }),
      "gringo identity preload completed",
    );
    expect(replyInfo).toHaveBeenCalledWith(
      expect.objectContaining({
        providerAccepted: true,
      }),
      "gringo unknown account message sent",
    );
  });

  it("injects full Gringo identity context once per direct session", async () => {
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    execFileMock.mockImplementation(
      (
        _bin: string,
        _args: string[],
        _options: unknown,
        callback: (error: null, stdout: string, stderr: string) => void,
      ) => {
        callback(
          null,
          JSON.stringify({
            ok: true,
            data: {
              prompt:
                "Trusted NaGringa coaching context\n\n# Alice - paid yearly\n\n- **Access:** paid yearly · DM: enabled",
              workingContext:
                "Trusted NaGringa working context\n- User: Alice · paid yearly · DM: enabled\n- Role: Senior Product Engineer\n- Coaching memory: loaded\n- Use this as the trusted identity/context card. Refresh only if asked or after profile/memory updates.",
            },
          }),
          "",
        );
      },
    );
    buildContextMock.mockImplementation((params: { bodyForAgent?: string }) => ({
      Body: "hi",
      BodyForAgent: params.bodyForAgent,
      RawBody: "hi",
      CommandBody: "hi",
      SessionKey: "agent:gringo:whatsapp:direct:+15550003333",
      Provider: "whatsapp",
      Surface: "whatsapp",
    }));
    const directMsg = {
      ...baseMsg,
      from: "+15550003333",
      conversationId: "+15550003333",
      chatId: "+15550003333",
      chatType: "direct",
      senderE164: "+15550003333",
      senderJid: "15550003333@s.whatsapp.net",
    };
    const directRoute = {
      ...baseRoute,
      agentId: "gringo",
      sessionKey: "agent:gringo:whatsapp:direct:+15550003333",
      mainSessionKey: "agent:gringo:whatsapp:direct:+15550003333",
    };

    await callProcessMessage({ msg: directMsg, route: directRoute });
    await callProcessMessage({ msg: directMsg, route: directRoute });

    expect(execFileMock).toHaveBeenCalledTimes(2);
    expect(execFileMock.mock.calls[0]?.[1]).toContain("context");
    expect(execFileMock.mock.calls[1]?.[1]).toContain("quota");
    expect(buildContextMock.mock.calls[0][0].bodyForAgent).toBe("hi");
    expect(trustedContextAt(0)).toContain("Trusted context for this turn is preloaded.");
    expect(trustedContextAt(0)).toContain("Trusted NaGringa coaching context");
    expect(trustedContextAt(0)).not.toContain("User message:\nhi");
    expect(buildContextMock.mock.calls[1][0].bodyForAgent).toBe("hi");
    expect(trustedContextAt(1)).not.toContain("Trusted NaGringa coaching context");
    expect(trustedContextAt(1)).toContain("Trusted NaGringa working context");
    expect(trustedContextAt(1)).toContain("Role: Senior Product Engineer");
    expect(trustedContextAt(1)).not.toContain("User message:\nhi");
  });

  it("refreshes full Gringo identity context on direct turn when requested", async () => {
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    execFileMock.mockImplementation(
      (
        _bin: string,
        _args: string[],
        _options: unknown,
        callback: (error: null, stdout: string, stderr: string) => void,
      ) => {
        callback(
          null,
          JSON.stringify({
            ok: true,
            data: {
              prompt:
                "Trusted NaGringa coaching context\n\n# Alice - paid yearly\n\n- **Access:** paid yearly · DM: enabled",
              workingContext:
                "Trusted NaGringa working context\n- User: Alice · paid yearly · DM: enabled",
            },
          }),
          "",
        );
      },
    );
    buildContextMock.mockImplementation((params: { bodyForAgent?: string }) => ({
      Body: params.bodyForAgent ?? "hi",
      BodyForAgent: params.bodyForAgent,
      RawBody: "hi",
      CommandBody: "hi",
      SessionKey: "agent:gringo:whatsapp:direct:+15550004444",
      Provider: "whatsapp",
      Surface: "whatsapp",
    }));
    const directMsg = {
      ...baseMsg,
      body: "hi",
      from: "+15550004444",
      conversationId: "+15550004444",
      chatId: "+15550004444",
      chatType: "direct",
      senderE164: "+15550004444",
      senderJid: "15550004444@s.whatsapp.net",
    };
    const directRoute = {
      ...baseRoute,
      agentId: "gringo",
      sessionKey: "agent:gringo:whatsapp:direct:+15550004444",
      mainSessionKey: "agent:gringo:whatsapp:direct:+15550004444",
    };

    await callProcessMessage({ msg: directMsg, route: directRoute });
    await callProcessMessage({
      msg: { ...directMsg, body: "atualiza meu contexto" },
      route: directRoute,
    });

    expect(execFileMock).toHaveBeenCalledTimes(2);
    expect(buildContextMock.mock.calls[1][0].bodyForAgent).toBe("atualiza meu contexto");
    expect(trustedContextAt(1)).toContain("Trusted NaGringa coaching context");
  });

  it("expires cached Gringo identity context after two hours", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-05-07T12:00:00Z"));
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    execFileMock.mockImplementation(
      (
        _bin: string,
        _args: string[],
        _options: unknown,
        callback: (error: null, stdout: string, stderr: string) => void,
      ) => {
        callback(
          null,
          JSON.stringify({
            ok: true,
            data: {
              prompt:
                "Trusted NaGringa coaching context\n\n# Alice - paid yearly\n\n- **Access:** paid yearly · DM: enabled",
              workingContext:
                "Trusted NaGringa working context\n- User: Alice · paid yearly · DM: enabled",
            },
          }),
          "",
        );
      },
    );
    buildContextMock.mockImplementation((params: { bodyForAgent?: string }) => ({
      Body: params.bodyForAgent ?? "hi",
      BodyForAgent: params.bodyForAgent,
      RawBody: "hi",
      CommandBody: "hi",
      SessionKey: "agent:gringo:whatsapp:direct:+15550005555",
      Provider: "whatsapp",
      Surface: "whatsapp",
    }));
    const directMsg = {
      ...baseMsg,
      body: "hi",
      from: "+15550005555",
      conversationId: "+15550005555",
      chatId: "+15550005555",
      chatType: "direct",
      senderE164: "+15550005555",
      senderJid: "15550005555@s.whatsapp.net",
    };
    const directRoute = {
      ...baseRoute,
      agentId: "gringo",
      sessionKey: "agent:gringo:whatsapp:direct:+15550005555",
      mainSessionKey: "agent:gringo:whatsapp:direct:+15550005555",
    };

    await callProcessMessage({ msg: directMsg, route: directRoute });
    vi.setSystemTime(new Date("2026-05-07T13:59:00Z"));
    await callProcessMessage({ msg: directMsg, route: directRoute });
    vi.setSystemTime(new Date("2026-05-07T14:01:00Z"));
    await callProcessMessage({ msg: directMsg, route: directRoute });

    expect(execFileMock).toHaveBeenCalledTimes(3);
    expect(execFileMock.mock.calls[1]?.[1]).toContain("quota");
    expect(buildContextMock.mock.calls[1][0].bodyForAgent).toBe("hi");
    expect(trustedContextAt(1)).toContain("Trusted NaGringa working context");
    expect(buildContextMock.mock.calls[2][0].bodyForAgent).toBe("hi");
    expect(trustedContextAt(2)).toContain("Trusted NaGringa coaching context");
  });

  it("tracks session metadata writes as connection background tasks", async () => {
    resolvePolicyMock.mockReturnValue(makePolicy(makeAccount()));
    buildContextMock.mockImplementationOnce(() => ({
      Body: "hi",
      RawBody: "hi",
      CommandBody: "hi",
      SessionKey: baseRoute.sessionKey,
      Provider: "whatsapp",
      Surface: "whatsapp",
    }));

    await callProcessMessage();

    expect(trackBackgroundTaskMock).toHaveBeenCalledTimes(1);
    expect(mockCallArg(trackBackgroundTaskMock, "trackBackgroundTask")).toBeInstanceOf(Set);
    expect(mockCallArg(trackBackgroundTaskMock, "trackBackgroundTask", 0, 1)).toBeInstanceOf(
      Promise,
    );
  });
});
