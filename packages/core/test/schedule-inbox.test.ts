import { describe, expect } from "bun:test"
import { Clock, DateTime, Effect, Exit, Layer, LayerMap, Sink, Stream } from "effect"
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
import { eq } from "drizzle-orm"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"
import { imagePassthrough } from "./lib/image"

const enabledDirectory = "/enabled"
const sessionID = Session.ID.make("ses_inbox_owner")
const testModel = Model.Ref.make({ providerID: Provider.ID.make("test"), id: Model.ID.make("test-model") })
const zeroTokens = TokenUsage.Info.make({ input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } })

const activeSessions = new Set<Session.ID>()
let onTurn: (sessionID: Session.ID) => Effect.Effect<string> = () => Effect.succeed("Done.")
const shellState = { exitCode: 0, stdout: "", stderr: "" }

const execution = makeGlobalNode({
  service: SessionExecution.Service,
  layer: Layer.effect(
    SessionExecution.Service,
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const bus = yield* Bus.Service
      const runTurn = Effect.fnUntraced(function* (targetSessionID: Session.ID) {
        activeSessions.add(targetSessionID)
        return yield* Effect.gen(function* () {
          yield* SessionInbox.promote(db, bus, targetSessionID, "input")
          const text = yield* onTurn(targetSessionID)
          const assistantMessageID = SessionMessage.ID.create()
          const now = yield* Clock.currentTimeMillis
          yield* bus.publish(SessionEvent.Step.Started, {
            sessionID: targetSessionID,
            assistantMessageID,
            agent: Agent.defaultID,
            model: testModel,
            started: now,
          })
          yield* bus.publish(SessionEvent.Text.Started, { sessionID: targetSessionID, assistantMessageID, ordinal: 0 })
          yield* bus.publish(SessionEvent.Text.Ended, {
            sessionID: targetSessionID,
            assistantMessageID,
            ordinal: 0,
            text,
          })
          yield* bus.publish(SessionEvent.Step.Ended, {
            sessionID: targetSessionID,
            assistantMessageID,
            finish: "stop",
            cost: Money.USD.zero,
            tokens: zeroTokens,
          })
          yield* bus.publish(SessionEvent.Execution.Succeeded, { sessionID: targetSessionID })
        }).pipe(Effect.ensuring(Effect.sync(() => activeSessions.delete(targetSessionID))))
      })
      return SessionExecution.Service.of({
        active: Effect.sync(() => new Set(activeSessions)),
        isActive: (id) => Effect.sync(() => activeSessions.has(id)),
        resume: (id) => runTurn(id) as unknown as ReturnType<SessionExecution.Interface["resume"]>,
        interrupt: () => Effect.succeed(false),
        wake: () => Effect.void,
        awaitIdle: () => Effect.void,
      })
    }),
  ),
  deps: [Database.node, Bus.node],
})

const enabled = Config.testLayer([
  new Document({
    type: "document",
    info: new Info({ timezone: "UTC", experimental: new ConfigExperimental.Info({ schedules: true }) }),
  }),
])

const testEnvironment = Layer.effect(
  Environment.Service,
  Effect.gen(function* () {
    const memory = Environment.makeMemoryDriver()
    const files = Environment.makeFiles(memory)
    yield* files.mkdir(enabledDirectory).pipe(Effect.orDie)
    const spawner = ChildProcessSpawner.make(() =>
      Effect.sync(() => {
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
    const allowAll: Permission.Ruleset = [{ action: "*", resource: "*", effect: "allow" }]
    yield* agents.transform((editor) => {
      editor.update(Agent.defaultID, (agent) => {
        agent.mode = "primary"
        agent.permissions = [...allowAll]
      })
      editor.update(Agent.ID.make("general"), (agent) => {
        agent.mode = "subagent"
        agent.permissions = [...allowAll]
      })
    })
  }),
)

const locations = makeGlobalNode({
  service: LocationServiceMap.Service,
  layer: Layer.effect(
    LocationServiceMap.Service,
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      const store = yield* SessionStore.Service
      const saved = yield* PermissionSaved.Service
      return yield* LayerMap.make(
        (ref: Location.Ref): Layer.Layer<LocationServices, FileSystem.DirectoryNotFoundError> =>
          // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
          Layer.mergeAll(
            enabled,
            configureLocation.pipe(
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
  deps: [Bus.node, SessionStore.node, PermissionSaved.node],
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
    [SessionExecution.node.replace(execution), LocationServiceMap.node.replace(locations)],
  ),
)

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
    activeSessions.clear()
    shellState.exitCode = 0
    shellState.stdout = ""
    shellState.stderr = ""
  })

describe("Schedule Run History and Inbox", () => {
  it.effect("delivers inbox-only runs without waking chat, orders needs_you first, and adapts cadence via schedule_result", () =>
    Effect.gen(function* () {
      yield* setup()
      const schedules = yield* Schedule.Service
      const { db } = yield* Database.Service

      const ownerPending = db
        .select()
        .from(SessionInboxTable)
        .where(eq(SessionInboxTable.session_id, sessionID))
        .all()
        .pipe(Effect.orDie)

      // Calling reportResult outside an active scheduled run fails cleanly.
      const outside = yield* Effect.exit(schedules.reportResult(sessionID, { attention: "quiet" }))
      expect(Exit.isFailure(outside)).toBe(true)

      const monitor = yield* schedules.create({
        sessionID,
        name: "Deploy monitor",
        when: { type: "every", every: "30 minutes" },
        action: {
          type: "subagent",
          prompt: "Check deploy health",
          deliver: "inbox",
        },
      })

      // Run #1 calls schedule_result({ attention: "quiet" }) despite returning verbose text -> stays out of inbox and chat.
      onTurn = (childID) =>
        schedules
          .reportResult(childID, { attention: "quiet" })
          .pipe(Effect.orDie, Effect.as("Checked 14 pods; all green, nothing to report."))
      yield* TestClock.adjust("30 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* ownerPending).toEqual([])
      expect(yield* schedules.inbox({ sessionID })).toEqual([])

      // Run #2 finishes with ready output -> appears in Inbox without waking ownerSession chat.
      onTurn = () => Effect.succeed("Canary finished cleanly at 100%.")
      yield* TestClock.adjust("30 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* ownerPending).toEqual([])
      expect(yield* schedules.inbox({ sessionID })).toMatchObject([
        { run: 2, attention: "ready", summary: "Canary finished cleanly at 100%." },
      ])

      // Run #3 calls schedule_result({ attention: "needs_you", summary, nextIn: "2 minutes" }):
      // - sorts ahead of ready in the Inbox
      // - overrides nextDueAt to +2m instead of +30m!
      onTurn = (childID) =>
        schedules
          .reportResult(childID, {
            attention: "needs_you",
            summary: "Migration needs production sign-off",
            nextIn: "2 minutes",
          })
          .pipe(Effect.orDie, Effect.as("Blocked on migration approval."))
      yield* TestClock.adjust("30 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect(yield* ownerPending).toEqual([])

      const afterAdaptive = yield* schedules.get(monitor.id)
      const nowMs = yield* Clock.currentTimeMillis
      expect(DateTime.toEpochMillis(afterAdaptive.nextDueAt!)).toBe(nowMs + 2 * 60_000)

      const inbox = yield* schedules.inbox({ sessionID })
      expect(inbox.map((item) => ({ run: item.run, attention: item.attention, summary: item.summary }))).toEqual([
        { run: 3, attention: "needs_you", summary: "Migration needs production sign-off" },
        { run: 2, attention: "ready", summary: "Canary finished cleanly at 100%." },
      ])

      // Marking run #3 read removes it from unread Inbox while keeping it in full run history.
      const readRun = yield* schedules.markRead(inbox[0]!.id)
      expect(readRun.readAt).toBeDefined()
      expect((yield* schedules.inbox({ sessionID })).map((item) => item.run)).toEqual([2])
      expect((yield* schedules.inbox({ sessionID, includeRead: true })).map((item) => item.run)).toEqual([3, 2])
      expect((yield* schedules.runs({ scheduleID: monitor.id })).map((item) => item.run)).toEqual([3, 2, 1])
    }),
  )

  it.effect("surfaces silent schedules in the Inbox when auto-paused after 3 consecutive failures", () =>
    Effect.gen(function* () {
      yield* setup()
      const schedules = yield* Schedule.Service

      const silent = yield* schedules.create({
        sessionID,
        name: "Nightly backup",
        when: { type: "every", every: "5 minutes" },
        action: { type: "shell", run: "bun run backup", deliver: "silent" },
      })
      shellState.exitCode = 1
      shellState.stderr = "disk full"

      for (const strike of [1, 2]) {
        yield* TestClock.adjust("5 minutes")
        yield* schedules.tick
        yield* Effect.repeat(Effect.yieldNow, { times: 5 })
        expect(yield* schedules.inbox({ sessionID })).toEqual([])
        expect((yield* schedules.runs({ scheduleID: silent.id }))[0]).toMatchObject({
          run: strike,
          status: "failed",
          attention: "quiet",
        })
      }

      yield* TestClock.adjust("5 minutes")
      yield* schedules.tick
      yield* Effect.repeat(Effect.yieldNow, { times: 5 })
      expect((yield* schedules.get(silent.id)).status).toBe("paused")
      expect(yield* schedules.inbox({ sessionID })).toMatchObject([
        {
          scheduleID: silent.id,
          run: 3,
          status: "failed",
          attention: "needs_you",
          summary: expect.stringContaining("[Paused after repeated failures]"),
        },
      ])
    }),
  )
})
