# pubsub-flavor-queue 待发队列无锁升级

## 背景与目标

将 wedb/wpubsub/src/subscribe_broker.rs 中的待发队列从 EventWorkQueue 升级为基于 crossfire::flavor::List 的无锁待发链表与 event_listener::Event 被动节流脉冲。
消除通道与互斥锁争用开销，实现完全无锁（zero-lock / CAS-free）入队与流式直接消费。

## 架构与核心实现

### 1. 依赖与数据结构

引入 crossfire::flavor::List 与 event_listener::Event。

在 SubscribeBroker 中：
- pending_queue 改为 crossfire::flavor::List<PendingEntry>
- pending_event 保留 event_listener::Event 用于被动脉冲唤醒

### 2. publish 入队

- 直接调用 pending_queue.push(entry)，完全无锁入队（zero-lock / CAS-free）
- 调用 pending_event.notify(1) 进行单发节流通知唤醒后台工作任务

### 3. wait_pending 异步等待

- 优先通过 disposed.load(Ordering::Acquire) 校验销毁状态，避免并发场景下活锁
- 若 pending_queue 非空立即返回 true
- 无任务且未销毁时，基于 pending_event.listen() 异步挂起等待
- 唤醒后再次确认 disposed 状态与队列状态

### 4. consume_pending 流式消费

- crossfire::flavor::List 底层基于 SegQueue，天然满足 FIFO 保序
- 采用 while let Some(entry) = self.pending_queue.pop() 流式出队并直接分发
- 消除中间过渡 Vec 堆分配，辅助内存复杂度降为 O(1)

### 5. clear 与 dispose 生命周期

- clear: 循环 pop 排空 pending_queue，并重置 channel_registry
- dispose: 原子置位 disposed，循环 pop 排空待发队列，调用 pending_event.notify(usize::MAX) 唤醒所有等待者优雅退出

### 6. 数据解码与防御性校验

- 针对 payload_slices 编码使用 split_first_chunk 处理 4 字节长度前缀
- 对空字节、截断及溢出长度增加防御分支，避免 panic

## 代码规范与审查优化

经子代理对照 ./.agents/skills/rust_review/SKILL.md 审查并落地以下优化：
- 消除 broadcast 派发重复逻辑，收敛至统一分发路径
- 消除 consume_pending 暂存 Vec 分配，就地流式处理并释放
- 完善边界单元测试，覆盖 publish 保序、wait_pending 销毁退出、截断畸形 payload 等场景

## 验证结论

- bun ./js/check.js: 0 缺失 0 重复
- ./clippy.sh: 0 警告
- ./test.sh: 全量单元测试与回归测试通过
