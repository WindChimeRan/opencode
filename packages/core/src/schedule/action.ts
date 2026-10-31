export * as ScheduleAction from "./action.js"

import type { Schedule as ScheduleSchema } from "@opencode/schema/schedule"
import { Effect, Option, Result, type Schema } from "effect"
import { Agent } from "../agent.js"
import { Command } from "../command.js"
import { Permission } from "../permission.js"
import { Plugin } from "../plugin.js"
import type { SessionMessage } from "../session/message.js"
import type { SessionSchema } from "../session/schema.js"
import { ScheduleCheck } from "./check.js"

const DEFAULT_SUBAGENT = Agent.ID.make("general")

export function validate(action: ScheduleSchema.Action): Result.Result<void, string> {
  switch (action.type) {
    case "prompt":
      if (action.prompt.trim() === "") return Result.fail("A prompt action needs a prompt")
      if ((action.thread ?? "same") === "same" && action.deliver === "silent")
        return Result.fail('deliver: "silent" requires a background action (subagent, dedicated, command, run or code)')
      return Result.succeed(undefined)
    case "subagent":
      if (action.prompt.trim() === "") return Result.fail("A subagent action needs a prompt")
      if (action.agent !== undefined && action.agent.trim() === "") return Result.fail("Agent name cannot be empty")
      return Result.succeed(undefined)
    case "command":
      if (action.command.trim() === "") return Result.fail("A command action needs a command name")
      if (action.command === "schedule") return Result.fail("Command schedule cannot be scheduled")
      if (action.target === "same" && action.deliver === "silent")
        return Result.fail('deliver: "silent" cannot be combined with target: "same"')
      return Result.succeed(undefined)
    case "shell":
      if (action.run.trim() === "") return Result.fail("A shell action needs a command")
      return Result.succeed(undefined)
    case "code":
      return ScheduleCheck.validateCode(action.code, "schedule action")
  }
}

export function defaultLabel(action: ScheduleSchema.Action): string {
  switch (action.type) {
    case "prompt":
      return action.thread === "dedicated" ? "Scheduled thread" : "Scheduled prompt"
    case "subagent":
      return "Scheduled subagent"
    case "command":
      return `/${action.command}`
    case "shell":
      return "Scheduled command"
    case "code":
      return "Scheduled script"
  }
}

export function summaryText(action: ScheduleSchema.Action): string {
  switch (action.type) {
    case "prompt":
    case "subagent":
      return action.prompt
    case "command":
      return action.arguments?.trim() ? `/${action.command} ${action.arguments.trim()}` : `/${action.command}`
    case "shell":
      return action.run
    case "code":
      return action.code
  }
}

export function isBackground(action: ScheduleSchema.Action): boolean {
  switch (action.type) {
    case "prompt":
      return action.thread === "dedicated"
    case "subagent":
    case "shell":
    case "code":
      return true
    case "command":
      return (action.target ?? "subagent") !== "same"
  }
}

export function usesDedicatedSession(action: ScheduleSchema.Action): boolean {
  return (
    (action.type === "prompt" && action.thread === "dedicated") ||
    (action.type === "command" && action.target === "dedicated")
  )
}

export function deliverMode(action: ScheduleSchema.Action): ScheduleSchema.Delivery {
  return action.deliver ?? "session"
}

export function isQuietOutput(text: string): boolean {
  const trimmed = text.trim()
  return trimmed === "" || /^NO_REPLY[.!]*$/i.test(trimmed)
}

/** Validates and authorizes action permissions at `Schedule.create` time while the user is present. */
export const authorize = Effect.fnUntraced(function* (
  session: SessionSchema.Info,
  name: string | undefined,
  when: ScheduleSchema.When,
  action: ScheduleSchema.Action,
) {
  if (action.type === "prompt" && action.thread !== "dedicated") return
  const plugins = yield* Plugin.Service
  yield* plugins.awaitActivation

  switch (action.type) {
    case "prompt": {
      const agents = yield* Agent.Service
      const permission = yield* Permission.Service
      const caller = yield* agents.select(session.agent)
      yield* permission.assert({
        action: "subagent",
        resources: [caller.id],
        metadata: { schedule: name, mode: "action", when, unattended: true, dedicated: true },
        sessionID: session.id,
        agent: caller.id,
      })
      return
    }
    case "subagent": {
      const agents = yield* Agent.Service
      const permission = yield* Permission.Service
      const caller = yield* agents.select(session.agent)
      const subagentID = yield* resolveSubagentID(agents, action.agent)
      yield* permission.assert({
        action: "subagent",
        resources: [subagentID],
        metadata: { schedule: name, mode: "action", when, unattended: true, agent: subagentID },
        sessionID: session.id,
        agent: caller.id,
      })
      return
    }
    case "command": {
      const commands = yield* Command.Service
      const found = yield* commands.definition(action.command)
      if (!found) return yield* Effect.fail(`Unknown command: ${action.command}`)
      for (const snippet of found.shell ?? []) {
        yield* ScheduleCheck.authorizeShellCommand(session, snippet, {
          schedule: name,
          mode: "action",
          when,
          unattended: true,
          command: action.command,
          run: snippet,
        })
      }
      const target = action.target ?? "subagent"
      if (target !== "same") {
        const agents = yield* Agent.Service
        const permission = yield* Permission.Service
        const caller = yield* agents.select(session.agent)
        const general = yield* agents.resolve(DEFAULT_SUBAGENT)
        const targetAgent = found.agent ?? (target === "subagent" ? (general?.id ?? caller.id) : caller.id)
        yield* permission.assert({
          action: "subagent",
          resources: [targetAgent],
          metadata: {
            schedule: name,
            mode: "action",
            when,
            unattended: true,
            command: action.command,
            ...(target === "dedicated" ? { dedicated: true } : {}),
          },
          sessionID: session.id,
          agent: caller.id,
        })
      }
      return
    }
    case "shell":
      yield* ScheduleCheck.authorizeShellCommand(session, action.run, {
        schedule: name,
        mode: "action",
        when,
        unattended: true,
        run: action.run,
      })
      return
    case "code":
      yield* ScheduleCheck.authorizeCodeProgram(
        session,
        action.code,
        {
          schedule: name,
          mode: "action",
          when,
          unattended: true,
        },
        { allowEdit: true, target: "schedule action" },
      )
      return
  }
})

export const selectSubagent = Effect.fnUntraced(function* (
  session: SessionSchema.Info,
  requestedAgent: string | undefined,
  commandName?: string,
) {
  const plugins = yield* Plugin.Service
  yield* plugins.awaitActivation
  const agents = yield* Agent.Service
  if (commandName !== undefined) {
    const commands = yield* Command.Service
    const found = yield* commands.definition(commandName)
    if (found?.agent !== undefined) {
      const resolved = yield* agents.resolve(found.agent)
      if (resolved) return { id: resolved.id, model: resolved.model ?? session.model }
    }
  }
  if (requestedAgent !== undefined) {
    const resolved = yield* agents.resolve(requestedAgent)
    if (!resolved) return yield* Effect.fail(new Error(`Unknown agent: ${requestedAgent}`))
    if (resolved.mode === "primary")
      return yield* Effect.fail(new Error(`Agent ${requestedAgent} cannot run as a subagent`))
    return { id: resolved.id, model: resolved.model ?? session.model }
  }
  const general = yield* agents.resolve(DEFAULT_SUBAGENT)
  if (general) return { id: general.id, model: general.model ?? session.model }
  const fallback = yield* agents.select(session.agent)
  return { id: fallback.id, model: fallback.info?.model ?? session.model }
})

const resolveSubagentID = Effect.fnUntraced(function* (agents: Agent.Interface, requested: string | undefined) {
  if (requested !== undefined) {
    const resolved = yield* agents.resolve(requested)
    if (!resolved) return yield* Effect.fail(`Unknown agent: ${requested}`)
    if (resolved.mode === "primary") return yield* Effect.fail(`Agent ${requested} cannot run as a subagent`)
    return resolved.id
  }
  const general = yield* agents.resolve(DEFAULT_SUBAGENT)
  if (general) return general.id
  const selected = yield* agents.select()
  return selected.id
})

export const runDeterministic = Effect.fnUntraced(function* (
  session: SessionSchema.Info,
  action: ScheduleSchema.ShellAction | ScheduleSchema.CodeAction,
  context: {
    readonly messageID: SessionMessage.ID
    readonly now: number
    readonly run: number
    readonly message?: string
  },
) {
  const plugins = yield* Plugin.Service
  yield* plugins.awaitActivation
  const task = Effect.gen(function* () {
    switch (action.type) {
      case "shell": {
        const executed = yield* ScheduleCheck.execShellCommand(session, action.run)
        if (Result.isFailure(executed)) return yield* Effect.fail(new Error(executed.failure))
        if (executed.success.exitCode !== 0) {
          const detail = executed.success.stderr.trim() || executed.success.stdout.trim()
          return yield* Effect.fail(
            new Error(
              `Command exited with code ${executed.success.exitCode}${detail ? `: ${detail}` : ""}`,
            ),
          )
        }
        return executed.success.stdout.trim()
      }
      case "code": {
        const bindings: Record<string, Schema.Json> = {
          now: context.now,
          run: context.run,
          ...(context.message === undefined ? {} : { message: context.message }),
          input: {
            now: context.now,
            run: context.run,
            ...(context.message === undefined ? {} : { message: context.message }),
          },
        }
        const executed = yield* ScheduleCheck.execCodeProgram(
          session,
          action.code,
          bindings,
          context.messageID,
          { allowEdit: true, target: "schedule action" },
        )
        if (!executed.ok) return yield* Effect.fail(new Error(executed.error))
        return formatCodeResult(executed.value)
      }
    }
  })

  const timed = yield* task.pipe(Effect.timeoutOption(ScheduleCheck.TIMEOUT_MS))
  return yield* Option.match(timed, {
    onNone: () => Effect.fail(new Error(`Scheduled action timed out after ${ScheduleCheck.TIMEOUT_MS}ms`)),
    onSome: Effect.succeed,
  })
})

export function extractTurnReply(
  messages: ReadonlyArray<SessionMessage.Info>,
  priorLatestID: SessionMessage.ID | undefined,
): Result.Result<string, string> {
  for (const message of messages) {
    if (priorLatestID !== undefined && message.id === priorLatestID) break
    if (message.type !== "assistant") continue
    if (message.error !== undefined) return Result.fail(message.error.message)
    if (message.time.completed === undefined) continue
    const text = message.content
      .filter((part) => part.type === "text")
      .map((part) => part.text.trim())
      .filter((part) => part.length > 0)
      .join("\n\n")
    return Result.succeed(text)
  }
  return Result.succeed("")
}

function formatCodeResult(value: unknown): string {
  if (value === undefined || value === null || value === false) return ""
  if (typeof value === "string") return value.trim()
  return JSON.stringify(value, null, 2)
}
