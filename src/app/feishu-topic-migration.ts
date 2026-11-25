/** 一次性拆分旧版合并的私聊线程，不重放历史输入或发送回执。 */
import { threadKey, type Decision, type Thread, type Trigger } from '../core/index.ts'
import { KV } from '../kv.ts'
import { retainedTopicRoot } from '../sources/feishu-topics.ts'
import type { Store } from './store.ts'

/** 根据根消息链及原生 thread_id 重建私聊线程、明确来源的订阅和索引。 */
export function migrateFeishuTopics(store: Store): void {
  if (store.get(KV.feishu.isolated) && store.get(KV.feishu.cutoverReady)) return
  const allTriggers = store.db.prepare('SELECT value FROM trigger').all().map(row => JSON.parse(String(row.value)) as Trigger)
  const decisions = store.db.prepare('SELECT value FROM decision').all().map(row => JSON.parse(String(row.value)) as Decision)
  const triggers = allTriggers.filter(trigger => trigger.source === 'feishu' && trigger.meta.direct === true
    && typeof trigger.meta.chatId === 'string' && typeof trigger.meta.messageId === 'string')
  const chats = new Set(triggers.map(trigger => String(trigger.meta.chatId)))
  // 新增轮询覆盖不能把升级前漏收的旧话题消息变成新请求。
  if (!store.get(KV.feishu.cutoverReady)) {
    const cutover = new Date().toISOString()
    for (const chat of chats) store.set(KV.feishu.cutover(chat), cutover)
    store.set(KV.feishu.cutoverReady, true)
  }
  if (store.get(KV.feishu.isolated)) return
  for (const chat of chats) {
    const messages = triggers.filter(trigger => trigger.meta.chatId === chat)
    const byMessage = new Map(messages.map(trigger => [String(trigger.meta.messageId), trigger]))
    const rootOf = (trigger: Trigger) => retainedTopicRoot(trigger, id => byMessage.get(id))
    const roots: Record<string, string> = {}
    for (const trigger of [...messages].sort((a, b) => a.at.localeCompare(b.at) || a.id.localeCompare(b.id))) {
      const native = trigger.meta.threadId
      if (typeof native !== 'string') continue
      const root = rootOf(trigger)
      // 原生话题共享一个根；优先保留能追溯到已存顶层消息的根。
      if (!roots[native] || (!byMessage.has(roots[native]!) && byMessage.has(root))) roots[native] = root
    }
    const aliases = new Map<string, string>()
    for (const trigger of messages) {
      const native = trigger.meta.threadId
      if (typeof native === 'string') aliases.set(rootOf(trigger), roots[native]!)
    }
    const original = store.threads.get(`feishu:${chat}`)
    const byTrigger = new Map<string, string>()
    const groups = new Map<string, Trigger[]>()
    for (const trigger of messages) {
      const root = aliases.get(rootOf(trigger)) ?? rootOf(trigger)
      const id = `${chat}:${root}`
      const updated = { ...trigger, threads: [{ source: 'feishu', id }] }
      store.triggers.save(updated)
      byTrigger.set(trigger.id, threadKey(updated.threads[0]!))
      groups.set(root, [...groups.get(root) ?? [], updated])
    }
    store.set(KV.feishu.roots(chat), roots)
    const subscriptions = new Map<string, Set<string>>()
    const subscribe = (ticket: string, trigger: string) => {
      const thread = byTrigger.get(trigger)
      if (thread) {
        const ids = subscriptions.get(thread) ?? new Set<string>()
        ids.add(ticket); subscriptions.set(thread, ids)
      }
    }
    for (const ticket of store.tickets.list()) {
      for (const trigger of ticket.origin.triggerIds) subscribe(ticket.id, trigger)
      subscribe(ticket.id, ticket.origin.triggerId)
    }
    // attach 的正式 Decision 才建立跨话题订阅，历史广播输入不代表订阅意图。
    for (const decision of decisions) {
      const ticket = (decision.kind === 'accept' ? decision.ticketId : undefined) ?? decision.reason.find(reason => reason.code === 'routed')?.values?.ticket
      if (ticket) for (const trigger of decision.triggerIds) subscribe(ticket, trigger)
    }
    const evaluating = original?.evaluating
    if (evaluating) store.db.prepare("DELETE FROM effect WHERE kind='assessment' AND key=?").run(evaluating.id)
    for (const [root, members] of groups) {
      const id = `${chat}:${root}`, key = `feishu:${id}`
      const previous = store.threads.get(key)
      const ids = new Set(members.map(trigger => trigger.id))
      const native = Object.keys(roots).find(value => roots[value] === root)
      const pending = [...new Set([...previous?.pending ?? [], ...original?.pending ?? [], ...evaluating?.triggerIds ?? []])].filter(value => ids.has(value))
      const thread: Thread = { key, ref: { source: 'feishu', id },
        createdAt: members.map(trigger => trigger.at).sort()[0]!, lastAt: members.map(trigger => trigger.at).sort().at(-1)!,
        subscriptions: [...new Set([...previous?.subscriptions ?? [], ...subscriptions.get(key) ?? []])],
        correlated: previous?.correlated ?? [], pending,
        meta: { ...previous?.meta, url: original?.meta.url ?? previous?.meta.url,
          ...original?.meta.facts ? { facts: original.meta.facts } : {}, ...native ? { threadId: native } : {},
          chatId: chat, messageId: root, sourceTriggerId: members.find(trigger => trigger.meta.messageId === root)?.id ?? members[0]!.id } }
      if (previous?.context) thread.context = previous.context
      store.threads.save(thread)
      if (original) {
        const seen = store.get(KV.feishu.seen(original.key))
        if (seen) store.set(KV.feishu.seen(key), seen)
        if (store.get(KV.feishu.initialized(original.key))) store.set(KV.feishu.initialized(key), true)
      }
    }
    if (original) {
      store.db.prepare('DELETE FROM thread WHERE key=?').run(original.key)
      store.delete(KV.feishu.seen(original.key)); store.delete(KV.feishu.initialized(original.key))
    }
    for (const decision of decisions) {
      if (decision.threadKey === `feishu:${chat}`) {
        const thread = byTrigger.get(decision.triggerIds[0]!)
        if (thread) store.decisions.save({ ...decision, threadKey: thread })
      }
    }
    for (const ticket of store.tickets.list()) {
      const allowed = (trigger: string) => {
        const thread = byTrigger.get(trigger)
        return !thread || subscriptions.get(thread)?.has(ticket.id)
      }
      ticket.undelivered = ticket.undelivered.filter(id => {
        const input = ticket.inputs.find(value => value.id === id)
        return input?.kind !== 'trigger' || allowed(input.triggerId)
      })
      if (ticket.answers) ticket.answers = ticket.answers.filter(allowed)
      if (ticket.follows?.some(follow => follow.thread === `feishu:${chat}`)) {
        ticket.follows = ticket.follows.flatMap(follow => follow.thread !== `feishu:${chat}` ? [follow]
          : [...subscriptions].filter(([, ids]) => ids.has(ticket.id)).map(([thread]) => ({ ...follow, thread })))
      }
      store.tickets.save(ticket)
    }
  }
  store.set(KV.feishu.isolated, true)
}
