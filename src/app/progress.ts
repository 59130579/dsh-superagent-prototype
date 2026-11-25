/** 在工单原始飞书话题中投递关键进展，并持久化限频、合并和去重状态。 */
import { key, latestReport, liveTicket, type DomainEvent, type OutboundMessage, type Ticket } from '../core/index.ts'
import { KV } from '../kv.ts'
import type { Outbox } from './outbox.ts'
import type { Store } from './store.ts'

export interface ProgressState {
  sequence: number
  started?: boolean
  lastText?: string
  sentAt?: number
  pending?: OutboundMessage
}
interface ProgressHost {
  readonly store: Pick<Store, 'get' | 'set' | 'keys' | 'atomic' | 'tickets' | 'triggers' | 'settings'>
  readonly outbox: Pick<Outbox, 'send' | 'withdraw' | 'status'>
  atCapacity(): boolean
}

/** 宿主统一生成、保存并投递各工单的公开进展。 */
export class Progress {
  constructor(private readonly host: ProgressHost) {}
  /** 工单首次开始和实际排队时产生状态通知；结束后撤回尚未发送的进展。 */
  event(event: DomainEvent): void {
    if (event.type !== 'ticket.created' && event.type !== 'assignment.started' && event.type !== 'ticket.ended') return
    const { ticketId } = event.data
    if (event.type === 'ticket.ended') { this.clear(ticketId); return }
    if (!this.host.store.settings().executor.progressUpdates) return
    const ticket = this.host.store.tickets.get(ticketId)
    if (!ticket) return
    const state = this.state(ticketId)
    if (event.type === 'assignment.started') {
      if (state.started) return
      this.host.store.set(KV.progress.ticket(ticketId), { ...state, started: true })
    } else if (!this.host.atCapacity()) return
    const chinese = /[\u3400-\u9fff]/.test(this.host.store.triggers.get(ticket.origin.triggerId)?.text ?? '')
    const text = event.type === 'ticket.created'
      ? chinese ? '任务已受理，正在排队等待执行。' : 'Accepted; waiting for an execution slot.'
      : chinese ? '已开始执行，正在核对任务上下文。' : 'Started; checking the task context.'
    this.publish(ticket, text)
  }
  /** 只接受 Worker 显式提供的公开摘要，不转发内部工程报告。 */
  publish(ticket: Ticket, text: string): 'queued' | 'disabled' | 'unavailable' | 'duplicate' {
    const { store, outbox } = this.host
    if (!store.settings().executor.progressUpdates) return 'disabled'
    const trigger = store.triggers.get(ticket.origin.triggerId)
    if (!liveTicket(ticket) || !trigger?.human || trigger.channel?.source !== 'feishu') return 'unavailable'
    return store.atomic(() => {
      const state = this.state(ticket.id)
      if (state.pending?.text === text) return 'duplicate'
      if (state.pending) outbox.withdraw(state.pending.id)
      if (state.lastText === text) {
        const { pending: _pending, ...kept } = state
        store.set(KV.progress.ticket(ticket.id), kept)
        return 'duplicate'
      }
      const sequence = state.sequence + 1
      const pending: OutboundMessage = { id: key('progress', ticket.id, String(sequence), text),
        channel: trigger.channel!, text, kind: 'progress', ticketId: ticket.id }
      store.set(KV.progress.ticket(ticket.id), { ...state, sequence, pending })
      this.flush(ticket.id)
      return 'queued'
    })
  }
  /** 设置变更和每次运行时调度都检查待发送进展；重新开启不会补发已撤回摘要。 */
  reconcile(): void {
    for (const name of this.host.store.keys(KV.progress.ticket)) {
      const ticketId = name.slice(KV.progress.ticket.prefix.length)
      this.flush(ticketId)
    }
  }
  /** 撤回尚未发送的摘要，保留历史发送状态以便重启后去重。 */
  clear(ticketId: string): void {
    const { store, outbox } = this.host
    store.atomic(() => {
      const state = this.state(ticketId)
      if (!state.pending) return
      outbox.withdraw(state.pending.id)
      const { pending: _pending, ...kept } = state
      store.set(KV.progress.ticket(ticketId), kept)
    })
  }
  /** 实际发送前再次校验开关、工单状态、摘要版本和最近成功发送时间。 */
  delivery(message: OutboundMessage): 'send' | 'wait' | 'cancel' {
    const { store } = this.host
    const ticket = message.ticketId ? store.tickets.get(message.ticketId) : undefined
    if (!ticket || !store.settings().executor.progressUpdates || !liveTicket(ticket) || latestReport(ticket)?.state === 'ready') return 'cancel'
    const state = this.state(ticket.id)
    if (state.pending?.id !== message.id) return 'cancel'
    const interval = store.settings().executor.progressIntervalSeconds * 1000
    return state.sentAt !== undefined && Date.now() < state.sentAt + interval ? 'wait' : 'send'
  }
  /** 成功发送时间决定下一次投递的最早时间，重试沿用原有回执。 */
  settled(message: OutboundMessage, sent: boolean): void {
    if (!message.ticketId) return
    const { store } = this.host
    store.atomic(() => {
      const state = this.state(message.ticketId!)
      const { pending, ...kept } = state
      store.set(KV.progress.ticket(message.ticketId!), { ...kept,
        ...pending?.id !== message.id ? { pending } : {},
        ...sent ? { lastText: message.text, sentAt: Date.now() } : {} })
    })
  }
  private state(ticketId: string): ProgressState { return this.host.store.get(KV.progress.ticket(ticketId)) ?? { sequence: 0 } }
  private flush(ticketId: string): void {
    const message = this.state(ticketId).pending
    if (!message) return
    const decision = this.delivery(message)
    if (decision === 'cancel') this.clear(ticketId)
    else if (decision === 'send' && !this.host.outbox.status(message.id)) this.host.outbox.send(message)
  }
}
