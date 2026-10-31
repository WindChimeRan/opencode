import { describe, expect } from "bun:test"
import { Clock, Deferred, Effect, Layer, LayerMap, Sink, Stream, Schema } from "effect"
import { ChildProcessSpawner } from "effect/process"
import { ExitCode, makeHandle, ProcessId } from "effect/process/ChildProcessSpawner"
import { TestClock } from "effect/testing"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Command } from "@opencode/core/command"
import { Config } from "@opencode/core/config"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Environment } from "@opencode/core/environment/index"
import { FileAccess } from "@opencode/core/file-access"
import type { FileSystem } from "@opencode/core/filesystem"
import { Image } from "@opencode/core/image"
import { Instance } from "@opencode/core/instance/service"
import { Job } from "@opencode/core/job"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import type { LocationServices } from "@opencode/core/location-services"
import { Permission } from "@opencode/core/permission"
import { PermissionSaved } from "@opencode/core/permission/saved"
import { Plugin } from "@opencode/core/plugin"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Schedule } from "@opencode/core/schedule"
import { Session } from "@opencode/core/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionInboxTable, SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { ShellSelect } from "@opencode/core/shell/select"
import { Skill } from "@opencode/core/skill"
import { Snapshot } from "@opencode/core/snapshot"
import { Tool } from "@opencode/core/tool"
import { McpTool } from "@opencode/core/tool/mcp"
import { Document, Info } from "@opencode/schema/config"
import { ConfigExperimental } from "@opencode/schema/config/experimental"
import { Model } from "@opencode/schema/model"
import { Money } from "@opencode/schema/money"
import { Provider } from "@opencode/schema/provider"
import { TokenUsage } from "@opencode/schema/token-usage"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"
import { imagePassthrough } from "./lib/image"

const enabledDirectory = "/enabled"
const testModel = Model.Ref.make({ providerID: Provider.ID.make("test"), id: Model.ID.make("test-model") })
const zeroTokens = TokenUsage.Info.make({ input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } })

const wakes: Session.ID[] = []
const activeSessions = new Set<Session.ID>()
let resumeReply: (sessionID: Session.ID) => Effect.Effect<string> = () => Effect.succeed("Done.")
let agentRules: Permission.Ruleset = [{ action: "*", resource: "*", effect: "allow" }]
const shellState = {
  exitCode: 0,
  stdout: "",
  stderr: "",
  runs: [] as string[],
}
const codeLog: Array<{ run: unknown; message: unknown }> = []

const execution = makeGlobalNode({
  service: SessionExecution.Service,
  layer: Layer.effect(
    SessionExecution.Service,
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const bus = yield* Bus.Service
      const runTurn = Effect.fnUntraced(function* (sessionID: Session.ID) {
        activeSessions.add(sessionID)
        return yield* Effect.gen(function* () {
          yield* SessionInbox.promote(db, bus, sessionID, "input")
          const text = yield* resumeReply(sessionID)
          const assistantMessageID = SessionMessage.ID.create()
          const now = yield* Clock.currentTimeMillis
          yield* bus.publish(SessionEvent.Step.Started, {
            sessionID,
            assistantMessageID,
            agent: Agent.defaultID,
            model: testModel,
            started: now,
          })
          yield* bus.publish(SessionEvent.Text.Started, { sessionID, assistantMessageID, ordinal: 0 })
          yield* bus.publish(SessionEvent.Text.Ended, { sessionID, assistantMessageID, ordinal: 0, text })
          yield* bus.publish(SessionEvent.Step.Ended, {
            sessionID,
            assistantMessageID,
            finish: "stop",
            cost: Money.USD.zero,
            tokens: zeroTokens,
          })
          yield* bus.publish(SessionEvent.Execution.Succeeded, { sessionID })
        }).pipe(Effect.ensuring(Effect.sync(() => activeSessions.delete(sessionID))))
      })
      return SessionExecution.Service.of({
        active: Effect.sync(() => new Set(activeSessions)),
        isActive: (id) => Effect.sync(() => activeSessions.has(id)),
        resume: (id) => runTurn(id) as unknown as ReturnType<SessionExecution.Interface["resume"]>,
        interrupt: () => Effect.succeed(false),
        wake: (id) =>
          Effect.sync(() => {
            wakes.push(id)
          }),
        awaitIdle: () => Effect.void,
      })
    }),
  ),
  deps: [Database.node, Bus.node],
})

const enabled = Config.testLayer([
  new Document({
    type: "document",
    info: new Info({
      timezone: "America/New_York",
      experimental: new ConfigExperimental.Info({ schedules: true }),
    }),
  }),
])

const testEnvironment = Layer.effect(
  Environment.Service,
  Effect.gen(function* () {
    const memory = Environment.makeMemoryDriver()
    const files = Environment.makeFiles(memory)
    yield* files.mkdir(enabledDirectory).pipe(Effect.orDie)
    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        const script = command._tag === "StandardCommand" ? String(command.args.at(-1) ?? command.command) : "pipeline"
        shellState.runs.push(script)
        const outBytes = new TextEncoder().encode(shellState.stdout)
        const errBytes = new TextEncoder().encode(shellState.stderr)
        return makeHandle({
          pid: ProcessId(1),
          exitCode: Effect.succeed(ExitCode(shellState.exitCode)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          stdin: Sink.drain,
          stdout: outBytes.byteLength > 0 ? Stream.make(outBytes) : Stream.empty,
          stderr: errBytes.byteLength > 0 ? Stream.make(errBytes) : Stream.empty,
          all: outBytes.byteLength > 0 ? Stream.make(outBytes) : Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        })
      }),
    )
    return Environment.Service.of({ files, spawner })
  }),
)

const configureLocation = Layer.effectDiscard(
  Effect.gen(function* () {
    const agents = yield* Agent.Service
    const commands = yield* Command.Service
    const tools = yield* Tool.Service
    const permission = yield* Permission.Service
    const inbox = yield* SessionInbox.Service

    yield* agents.transform((editor) => {
      editor.update(Agent.defaultID, (agent) => {
        agent.mode = "primary"
        agent.permissions = [...agentRules]
      })
      editor.update(Agent.ID.make("general"), (agent) => {
        agent.mode = "subagent"
        agent.permissions = [...agentRules]
      })
      editor.update(Agent.ID.make("reviewer"), (agent) => {
        agent.mode = "subagent"
        agent.permissions = [...agentRules]
      })
    })

    yield* commands.transform((editor) => {
      editor.add({
        name: "morning-brief",
        description: "Render morning brief",
        execute: (invocation) =>
          inbox
            .admit({
              id: SessionMessage.ID.create(),
              sessionID: invocation.sessionID,
              item: {
                type: "user",
                delivery: invocation.delivery,
                payload: SessionInbox.UserPayload.make({ text: `Brief for: ${invocation.prompt.text}` }),
              },
            })
            .pipe(Effect.asVoid),
      })
    })
    yield* commands.reload()

    yield* tools.transform((editor) => {
      editor.add({
        name: "post_digest",
        description: "Post digest",
        input: Schema.Struct({ run: Schema.Unknown, message: Schema.optionalKey(Schema.Unknown) }),
        output: Schema.Struct({ ok: Schema.Boolean }),
        options: { namespace: "slack" },
        execute: (input, context) =>
          permission
            .assert({
              action: "slack_post_digest",
              resources: ["*"],
              sessionID: context.sessionID,
              agent: context.agent,
            })
            .pipe(
              Effect.orDie,
              Effect.map(() => {
                codeLog.push({ run: input.run, message: input.message })
                return { output: { ok: true } }
              }),
            ),
      })
    })
  }),
)

const locations = makeGlobalNode({
  service: LocationServiceMap.Service,
  layer: Layer.effect(
    LocationServiceMap.Service,
    Effect.gen(function* () {
      const inbox = yield* SessionInbox.Service
      const bus = yield* Bus.Service
      const store = yield* SessionStore.Service
      const saved = yield* PermissionSaved.Service
      return yield* LayerMap.make(
        (ref: Location.Ref): Layer.Layer<LocationServices, FileSystem.DirectoryNotFoundError> =>
          // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
          Layer.mergeAll(
            enabled,
            configureLocation.pipe(
              Layer.provideMerge(Layer.succeed(SessionInbox.Service, inbox)),
              Layer.provideMerge(
                LayerNode.compile(
                  LayerNode.group([
                    Location.node,
                    Environment.node,
                    PluginHooks.node,
                    Skill.node,
                    Command.node,
                    Tool.node,
                    Agent.node,
                    Permission.node,
                    FileAccess.node,
                  ]),
                  {
                    replacements: [
                      Bus.node.replace(Layer.succeed(Bus.Service, bus)),
                      SessionStore.node.replace(Layer.succeed(SessionStore.Service, store)),
                      PermissionSaved.node.replace(Layer.succeed(PermissionSaved.Service, saved)),
                      Location.node.replace(Layer.succeed(Location.Service, location(ref))),
                      Environment.node.replace(testEnvironment),
                      Image.node.replace(imagePassthrough),
                    ],
                  },
                ),
              ),
            ),
            Layer.mock(McpTool.Service, { flush: Effect.void }),
            Layer.succeed(
              ShellSelect.Service,
              ShellSelect.Service.of({
                transform: () => Effect.die("unused"),
                reload: () => Effect.void,
                resolve: () => Effect.succeed("/bin/bash"),
              }),
            ),
            Layer.mock(Snapshot.Service, { capture: () => Effect.undefined, restore: () => Effect.void }),
            Layer.succeed(Plugin.Service, { awaitActivation: Effect.void } as unknown as Plugin.Interface),
          ).pipe(Layer.fresh) as unknown as Layer.Layer<LocationServices, FileSystem.DirectoryNotFoundError>,
      )
    }),
  ),
  deps: [SessionInbox.node, Bus.node, SessionStore.node, PermissionSaved.node],
})

const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      Bus.node,
      Job.node,
      SessionExecution.node,
      SessionProjector.node,
      SessionStore.node,
      Session.node,
      Schedule.node,
      Instance.node,
      LocationServiceMap.node,
    ]),
    [
      Bus.node.replace(Bus.configured({ persist: true })),
      SessionExecution.node.replace(execution),
      LocationServiceMap.node.replace(locations),
    ],
  ),
)

const sessionID = Session.ID.make("ses_owner")

const setup = () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "owner",
        directory: enabledDirectory,
        title: "owner",
        version: "test",
      })
      .run()
      .pipe(Effect.orDie)
    wakes.length = 0
    activeSessions.clear()
    resumeReply = () => Effect.succeed("Done.")
    agentRules = [{ action: "*", resource: "*", effect: "allow" }]
    shellState.exitCode = 0
    shellState.stdout = ""
    shellState.stderr = ""
    shellState.runs = []
    codeLog.length = 0
    return yield* Schedule.Service
  })

const ownerPending = Database.Service.use(({ db }) =>
  db
    .select()
    .from(SessionInboxTable)
    .all()
    .pipe(
      Effect.orDie,
      Effect.map((rows) => rows.filter((row) => row.session_id === sessionID)),
    ),
)

const promoteOwner = Effect.gen(function* () {
  const { db } = yield* Database.Service
  yield* SessionInbox.promote(db, yield* Bus.Service, sessionID, "input")
})

describe("Schedule background actions", () => {
  it.effect("runs subagent actions in an isolated child session, skips NO_REPLY, and delivers findings as queued notices", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const sessions = yield* Session.Service
      const schedule = yield* schedules.create({
        sessionID,
        name: "Triage",
        when: { type: "every", every: "5 minutes" },
        action: { type: "subagent", prompt: "Check regressions", agent: "reviewer" },
      })

      // Run #1 replies NO_REPLY -> finishes quietly without posting to ownerSession.
      resumeReply = () => Effect.succeed("NO_REPLY")
      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* ownerPending).toEqual([])
      expect(yield* schedules.get(schedule.id)).toMatchObject({
        runCount: 1,
        activeJobID: undefined,
        lastOutcome: "fired",
      })

      // Run #2 reports an actionable finding -> posts one queued synthetic notice to ownerSession.
      resumeReply = () => Effect.succeed("PR #42 broke main.")
      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      const pending = yield* ownerPending
      expect(pending).toHaveLength(1)
      expect(pending[0]).toMatchObject({
        type: "synthetic",
        delivery: "queue",
        payload: {
          description: "Triage · #2",
          text: expect.stringContaining("PR #42 broke main."),
        },
      })
      const children = (yield* sessions.list({ parentID: sessionID })).data
      expect(children).toHaveLength(2)
      expect(children[0]?.agent).toBe(Agent.ID.make("reviewer"))
    }),
  )

  it.effect("keeps foreground session non-busy while a background subagent runs and skips overlapping slots", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const exec = yield* SessionExecution.Service
      const gate = yield* Deferred.make<string>()
      resumeReply = () => Deferred.await(gate)

      const schedule = yield* schedules.create({
        sessionID,
        name: "Slow sweep",
        when: { type: "every", every: "1 minute" },
        action: { type: "subagent", prompt: "Audit slow tests" },
      })

      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      // Foreground ownerSession stays completely idle while the child job is in flight.
      expect(yield* exec.isActive(sessionID)).toBe(false)
      expect(yield* schedules.get(schedule.id)).toMatchObject({
        runCount: 1,
        activeJobID: `job_${schedule.id}_1`,
      })

      // Next slot while Run #1 is still in flight is skipped rather than overlapping.
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      expect(yield* schedules.get(schedule.id)).toMatchObject({
        runCount: 1,
        lastOutcome: "skipped",
        activeJobID: `job_${schedule.id}_1`,
      })

      // Unblocking Run #1 clears activeJobID and delivers the completion notice.
      yield* Deferred.succeed(gate, "Sweep complete.")
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* schedules.get(schedule.id)).toMatchObject({
        runCount: 1,
        activeJobID: undefined,
      })
      expect(yield* ownerPending).toHaveLength(1)

      // Even while Run #1's completion notice is still pending in the busy owner session, Run #2 fires on schedule.
      resumeReply = () => Effect.succeed("Sweep #2 complete.")
      yield* TestClock.adjust("1 minute")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* schedules.get(schedule.id)).toMatchObject({
        runCount: 2,
        lastOutcome: "fired",
        activeJobID: undefined,
      })
      expect(yield* ownerPending).toHaveLength(2)
    }),
  )

  it.effect("reuses one persistent dedicated session across runs and honors deliver: silent", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const sessions = yield* Session.Service
      const seenSessions: Session.ID[] = []
      resumeReply = (id) =>
        Effect.sync(() => {
          seenSessions.push(id)
          return `Memory turn ${seenSessions.length}`
        })

      const schedule = yield* schedules.create({
        sessionID,
        name: "Watcher",
        when: { type: "every", every: "5 minutes" },
        action: { type: "prompt", prompt: "Update rolling notes", thread: "dedicated", deliver: "silent" },
      })

      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      const afterFirst = yield* schedules.get(schedule.id)
      const dedicatedID = afterFirst.dedicatedSessionID!
      expect(dedicatedID).toStartWith("ses_")
      expect(dedicatedID).not.toBe(sessionID)
      expect(yield* ownerPending).toEqual([])

      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      const afterSecond = yield* schedules.get(schedule.id)
      expect(afterSecond.dedicatedSessionID).toBe(dedicatedID)
      expect(seenSessions).toEqual([dedicatedID, dedicatedID])
      expect(yield* ownerPending).toEqual([])

      const history = yield* sessions.messages({ sessionID: dedicatedID })
      expect(history.filter((m) => m.type === "assistant")).toHaveLength(2)
    }),
  )

  it.effect("executes scheduled slash commands in a background subagent without injecting kickoff prompts into the chat", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()
      const sessions = yield* Session.Service
      let childPrompt = ""
      resumeReply = (id) =>
        sessions.messages({ sessionID: id }).pipe(
          Effect.orDie,
          Effect.map((msgs) => {
            const first = msgs.find((m) => m.type === "user")
            childPrompt = first?.type === "user" ? first.text : ""
            return "Morning brief ready: 3 items."
          }),
        )

      yield* schedules.create({
        sessionID,
        name: "Brief",
        when: { type: "in", in: "10 minutes" },
        action: { type: "command", command: "morning-brief", arguments: "today" },
      })

      yield* TestClock.adjust("10 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(childPrompt).toBe("Brief for: today")

      // Owner session never received the command's raw template; only the compact completion notice was queued.
      const pending = yield* ownerPending
      expect(pending).toHaveLength(1)
      expect(pending[0]).toMatchObject({
        type: "synthetic",
        delivery: "queue",
        payload: {
          description: "Brief",
          text: expect.stringContaining("Morning brief ready: 3 items."),
        },
      })

      // Unknown commands fail validation at creation time.
      expect(
        yield* Effect.flip(
          schedules.create({
            sessionID,
            when: { type: "in", in: "5 minutes" },
            action: { type: "command", command: "does-not-exist" },
          }),
        ),
      ).toMatchObject({ _tag: "Schedule.InvalidError", reason: "Unknown command: does-not-exist" })
    }),
  )

  it.effect("runs deterministic shell and code actions with creation-time permissions and auto-pauses after 3 consecutive failures", () =>
    Effect.gen(function* () {
      const schedules = yield* setup()

      // Denied shell or subagent permission blocks creation up front (including dedicated autonomous threads).
      agentRules = [
        { action: "shell", resource: "*", effect: "deny" },
        { action: "subagent", resource: "*", effect: "deny" },
      ]
      expect(
        yield* Effect.flip(
          schedules.create({
            sessionID,
            when: { type: "in", in: "5 minutes" },
            action: { type: "prompt", prompt: "autonomous thread", thread: "dedicated" },
          }),
        ),
      ).toMatchObject({ _tag: "Permission.BlockedError" })
      expect(
        yield* Effect.flip(
          schedules.create({
            sessionID,
            when: { type: "in", in: "5 minutes" },
            action: { type: "shell", run: "echo blocked" },
          }),
        ),
      ).toMatchObject({ _tag: "Permission.BlockedError" })

      agentRules = [{ action: "*", resource: "*", effect: "allow" }]
      const shellSchedule = yield* schedules.create({
        sessionID,
        name: "Sync",
        when: { type: "every", every: "5 minutes" },
        action: { type: "shell", run: "git fetch origin" },
      })

      // Empty stdout finishes quietly without waking ownerSession.
      shellState.exitCode = 0
      shellState.stdout = ""
      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      expect(shellState.runs).toEqual(["git fetch origin"])
      expect(yield* ownerPending).toEqual([])

      // Non-empty stdout queues a completion notice.
      shellState.stdout = "Fetched 2 branches\n"
      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* ownerPending).toHaveLength(1)
      yield* promoteOwner
      yield* schedules.pause(shellSchedule.id)

      // 3 consecutive silent failures auto-pause the schedule and surface a paused notice once.
      const failingSilent = yield* schedules.create({
        sessionID,
        name: "Nightly",
        when: { type: "every", every: "5 minutes" },
        action: { type: "shell", run: "bun run broken", deliver: "silent" },
      })
      shellState.exitCode = 2
      shellState.stderr = "boom"

      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* ownerPending).toEqual([])
      expect(yield* schedules.get(failingSilent.id)).toMatchObject({ status: "active", lastOutcome: "failed" })

      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* ownerPending).toEqual([])
      expect(yield* schedules.get(failingSilent.id)).toMatchObject({ status: "active", lastOutcome: "failed" })

      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* schedules.get(failingSilent.id)).toMatchObject({ status: "paused", lastOutcome: "failed" })
      const pausedNotice = yield* ownerPending
      expect(pausedNotice).toHaveLength(1)
      expect(pausedNotice[0]?.payload).toMatchObject({
        description: "Nightly · paused",
        text: expect.stringContaining("[Schedule paused after 3 failed runs]"),
      })
      yield* promoteOwner

      // Code action receives check message and run number, and fires immediately if an until check matches at creation.
      shellState.exitCode = 0
      shellState.stdout = "PR #99 merged\n"
      const immediateCode = yield* schedules.create({
        sessionID,
        name: "Notify merge",
        until: { type: "shell", run: "gh pr view 99" },
        action: {
          type: "code",
          code: `await tools.slack.post_digest({ run, message }); return "Posted to Slack"`,
        },
      })
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(immediateCode).toMatchObject({ status: "done", runCount: 1 })
      expect(codeLog).toEqual([{ run: 1, message: "PR #99 merged" }])
      expect(yield* ownerPending).toHaveLength(1)
      expect((yield* schedules.get(shellSchedule.id)).status).toBe("paused")
    }),
  )
})
