/** 关键进展的运行时开关、话题投递、限频和恢复验证。 */
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { Commands } from '../src/infra/commands.ts'
import { Runtime } from '../src/app/runtime.ts'
import { feishuKind } from '../src/sources/feishu.ts'
import { githubKind } from '../src/sources/github.ts'
import { testMachine } from './fixtures/deployment.ts'
import { feishu, startRuntime, type Host } from './fixtures/host.ts'

let h: Host
beforeEach(async () => {
  vi.useFakeTimers({ toFake: ['Date'], now: Date.now() })
  h = await startRuntime()
})
afterEach(async () => { await h.runtime.stop(); await h.stop(); vi.useRealTimers() })
const lark = () => vi.mocked(Commands.prototype.lark)
const replies = () => lark().mock.calls.map(([args]) => args).filter(args => args[1] === '+messages-reply')
const containing = (text: string) => replies().filter(args => args.includes(text))
async function toggle(enabled: boolean) {
  const { store } = h.runtime
  const result = await h.api('PUT', 'settings/executor', { revision: store.blockRevision('executor'),
    value: { ...store.settings().executor, progressUpdates: enabled, progressIntervalSeconds: 45 } })
  expect(result).toMatchObject({ status: 200, body: { restarting: false, policy_changed: false } })
  await h.runtime.idle()
}
async function advance(ms = 45_000) {
  vi.setSystemTime(Date.now() + ms)
  await h.runtime.pump(); await h.runtime.sendReplies()
}
async function report(sessionId: string, text: string) {
  const result = await h.runTool(sessionId, 'orchestrator_report', { state: 'progress',
    summary: 'Internal evidence and diagnostic details.', public_summary: text })
  await h.runtime.idle()
  return result
}

it('keeps progress off by default and saves the switch without restarting or changing authorization', async () => {
  expect(h.runtime.store.settings().executor).toMatchObject({ progressUpdates: false, progressIntervalSeconds: 45 })
  await toggle(true)
  expect((await h.api('GET', 'settings')).body).toMatchObject({ blocks: { executor: { value: { progressUpdates: true } } } })
  await toggle(false)
})

it('retains internal reports while the progress switch is off', async () => {
  const { ticket, sessionId } = await h.acceptedTicket(feishu('off', '修复设置'))
  lark().mockClear()
  await report(sessionId, '已确认原因，正在修改。')
  expect(replies()).toHaveLength(0)
  expect(h.runtime.store.tickets.get(ticket.id)?.assignments.at(-1)?.report?.summary).toContain('Internal evidence')
})

it('posts lifecycle and public milestones only to each ticket original Feishu topic', async () => {
  await toggle(true)
  const a = await h.acceptedTicket(feishu('topic-a', '修复 A', { chat_type: 'p2p', thread_id: 'omt_a', root_id: 'om_topic-a' }))
  const b = await h.acceptedTicket(feishu('topic-b', '修复 B', { chat_type: 'p2p', thread_id: 'omt_b', root_id: 'om_topic-b' }))
  expect(containing('已开始执行，正在核对任务上下文。').map(args => args[args.indexOf('--message-id') + 1])).toEqual(['om_topic-a', 'om_topic-b'])
  await advance()
  lark().mockClear(); h.deliveries.mockClear()
  await report(a.sessionId, 'A 已定位，正在修改。')
  await report(b.sessionId, 'B 修改完成，正在验证。')
  for (const [text, target] of [['A 已定位，正在修改。', 'om_topic-a'], ['B 修改完成，正在验证。', 'om_topic-b']]) {
    expect(containing(text!)[0]).toEqual(expect.arrayContaining(['--message-id', target, '--reply-in-thread', '--idempotency-key']))
  }
  expect(JSON.stringify(replies())).not.toContain('Internal evidence')
  expect(h.relays(h.runtime.masterId).filter(item => item.kind === 'worker_report')).toHaveLength(0)
})

it('coalesces rapid milestones to the latest one and suppresses unchanged updates', async () => {
  await toggle(true)
  const { sessionId } = await h.acceptedTicket(feishu('coalesce', '修复设置'))
  lark().mockClear()
  await report(sessionId, '已确认原因。')
  await report(sessionId, '修改完成，正在验证。')
  expect(replies()).toHaveLength(0)
  await advance()
  expect(containing('已确认原因。')).toHaveLength(0)
  expect(containing('修改完成，正在验证。')).toHaveLength(1)
  await advance()
  await report(sessionId, '修改完成，正在验证。')
  expect(containing('修改完成，正在验证。')).toHaveLength(1)
})

it('shows actual queueing when another ticket occupies the worker capacity', async () => {
  await toggle(true)
  const { store } = h.runtime
  await h.runtime.settings.save('executor', { ...store.settings().executor, maxWorkers: 1 }, store.blockRevision('executor'), 'panel')
  await h.acceptedTicket(feishu('running', '修复 A'))
  lark().mockClear()
  await h.acceptedTicket(feishu('queued', '修复 B'))
  expect(containing('任务已受理，正在排队等待执行。')[0]).toEqual(expect.arrayContaining(['--message-id', 'om_queued']))
  expect(containing('已开始执行，正在核对任务上下文。')).toHaveLength(0)
})

it('keeps ordinary bookkeeping internal and rejects a milestone that duplicates a ready result or question', async () => {
  await toggle(true)
  const { sessionId } = await h.acceptedTicket(feishu('internal', '修复设置'))
  await advance(); lark().mockClear()
  await h.runTool(sessionId, 'orchestrator_report', { state: 'progress', summary: 'Binding an unfinished PR internally.' })
  await h.runtime.idle()
  expect(replies()).toHaveLength(0)
  await expect(h.runTool(sessionId, 'orchestrator_report', { state: 'ready', summary: 'Done.', public_summary: '正在验证。' })).rejects.toThrow('cannot duplicate')
  await expect(h.runTool(sessionId, 'orchestrator_report', { state: 'progress', summary: 'Needs a choice.', public_summary: '需要选择。', human_action: 'Which target?' })).rejects.toThrow('cannot duplicate')
})

it('cancels waiting milestones when disabled and does not replay them after re-enabling', async () => {
  await toggle(true)
  const { sessionId } = await h.acceptedTicket(feishu('toggle', '修复设置'))
  lark().mockClear()
  await report(sessionId, '修改完成，正在验证。')
  await toggle(false); await advance(); await toggle(true); await advance()
  expect(replies()).toHaveLength(0)
  await report(sessionId, '已完成新的检查。')
  expect(containing('已完成新的检查。')).toHaveLength(1)
})

it('drops pending progress once a ready outcome or a human stop supersedes it', async () => {
  await toggle(true)
  const a = await h.acceptedTicket(feishu('ready', '修复 A'))
  const b = await h.acceptedTicket(feishu('stopped', '修复 B'))
  lark().mockClear()
  await report(a.sessionId, 'A 修改完成，正在验证。')
  await report(b.sessionId, 'B 已定位。')
  await h.runTool(a.sessionId, 'orchestrator_report', { state: 'ready', summary: 'Verified and ready.' })
  await h.api('POST', `tickets/${b.ticket.id}/stop`)
  await advance()
  expect(containing('A 修改完成，正在验证。')).toHaveLength(0)
  expect(containing('B 已定位。')).toHaveLength(0)
  await h.runTool(h.runtime.masterId, 'orchestrator_reply', { purpose: 'result', ticket_id: a.ticket.id, text: '验证通过，结果已提交。' })
  await h.runtime.sendReplies()
  expect(containing('验证通过，结果已提交。')).toHaveLength(1)
})

it('retries a transient send with the same receipt and transport idempotency key', async () => {
  await toggle(true)
  const { sessionId } = await h.acceptedTicket(feishu('retry', '修复设置'))
  await advance()
  lark().mockClear(); lark().mockRejectedValueOnce(new Error('Temporary network failure'))
  await report(sessionId, '已确认原因，正在修改。')
  await advance(5000)
  const calls = containing('已确认原因，正在修改。')
  expect(calls).toHaveLength(2)
  expect(calls[0]).toEqual(calls[1])
  const id = calls[0]![calls[0]!.indexOf('--idempotency-key') + 1]!
  expect(h.runtime.outbox.status(id)).toBe('sent')
})

it('preserves the enabled switch and sent-milestone deduplication across a host restart', async () => {
  await toggle(true)
  const { sessionId } = await h.acceptedTicket(feishu('restart', '修复设置'))
  await advance(); await report(sessionId, '修改完成，正在验证。')
  await h.runtime.stop()
  const restored = new Context()
  for (const name of ['agents', 'llm', 'sessionController', 'sessionPersistence', 'workspaceRegistry', 'permissionPresets', 'subprocess', 'webServer'] as const) {
    restored.reflect.provide(name, h.ctx[name])
  }
  restored.reflect.provide('jobs', h.jobs.service); restored.reflect.provide('subagents', h.subagents.service)
  h.runtime = new Runtime(restored, testMachine({ stateDir: h.directory }), vi.fn())
  h.runtime.sources.register(feishuKind); h.runtime.sources.register(githubKind)
  await h.runtime.ready
  expect(h.runtime.store.settings().executor).toMatchObject({ progressUpdates: true })
  await h.runtime.idle(); await advance(); lark().mockClear()
  await report(sessionId, '修改完成，正在验证。')
  expect(replies()).toHaveLength(0)
})
