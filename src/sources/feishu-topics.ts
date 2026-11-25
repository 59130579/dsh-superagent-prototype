/** 私聊话题的根消息解析，统一实时事件、历史补收和轮询的线程归属。 */
import { z } from 'zod'
import type { Trigger } from '../core/index.ts'
import { KV } from '../kv.ts'
import type { SourceContext } from './host.ts'
import { feishuThreadId, feishuTriggerId } from './feishu-trigger.ts'

/** 沿已知根消息追溯顶层消息，拒绝循环引用。 */
export function retainedTopicRoot(trigger: Trigger, get: (message: string) => Trigger | undefined): string {
  let current = trigger
  const seen = new Set<string>()
  for (;;) {
    const message = String(current.meta.messageId)
    if (seen.has(message)) throw new Error('Feishu topic contains a root-message cycle')
    seen.add(message)
    const root = typeof current.meta.rootId === 'string' ? current.meta.rootId : undefined
    if (!root || root === message) return message
    const parent = get(root)
    if (!parent) return root
    if (parent.meta.chatId !== trigger.meta.chatId) throw new Error('Feishu topic root belongs to another chat')
    current = parent
  }
}

/** 用已验证的原生话题映射归一私聊消息；未知机器人根消息通过只读 API 追溯。 */
export async function normalizeFeishuTopic(ctx: SourceContext, trigger: Trigger): Promise<Trigger> {
  if (trigger.meta.direct !== true) return trigger
  const chat = String(trigger.meta.chatId)
  const native = typeof trigger.meta.threadId === 'string' ? trigger.meta.threadId : undefined
  let root = native ? ctx.state.get(KV.feishu.roots(chat))?.[native] : undefined
  // 同一消息的补读可能省略 root_id；保留首次接收时验证过的话题归属。
  const retained = ctx.triggers.get(trigger.id)
  const prior = retained?.threads.find(ref => ref.source === 'feishu' && ref.id.startsWith(`${chat}:`))
  root ??= prior?.id.slice(chat.length + 1)
  if (!root) {
    const known = new Map<string, Trigger>([[String(trigger.meta.messageId), trigger]])
    const get = (id: string) => known.get(id) ?? ctx.triggers.get(feishuTriggerId(id))
    root = retainedTopicRoot(trigger, get)
    const source = z.object({ items: z.array(z.object({ message_id: z.string(), chat_id: z.string(),
      root_id: z.string().optional(), deleted: z.boolean().optional() })) })
    const seen = new Set<string>()
    while (root !== trigger.meta.messageId && !get(root)) {
      if (seen.has(root) || seen.size >= 32) throw new Error('Feishu topic root could not be resolved')
      seen.add(root)
      const result = source.parse(await ctx.commands.lark(['api', 'GET', '/open-apis/im/v1/messages/mget',
        '--params', JSON.stringify({ message_ids: [root] })]))
      const message = result.items.find(item => item.message_id === root)
      if (!message || message.deleted || message.chat_id !== chat) throw new Error('Feishu topic root is unavailable or belongs to another chat')
      known.set(root, { ...trigger, meta: { chatId: chat, messageId: root, ...message.root_id ? { rootId: message.root_id } : {} } })
      root = retainedTopicRoot(trigger, get)
    }
  }
  const id = feishuThreadId(chat, true, root, String(trigger.meta.messageId))
  return ctx.atomic(() => {
    if (native) ctx.state.set(KV.feishu.roots(chat), { ...ctx.state.get(KV.feishu.roots(chat)), [native]: root! })
    return { ...trigger, threads: [{ source: 'feishu', id }] }
  })
}
