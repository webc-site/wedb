优先级：低

waof 清单死依赖：event-listener 声明于 [dependencies] 但 src/tests 全零引用
  waof/Cargo.toml:22 `event-listener = { workspace = true }`，waof/src 与 waof/tests 全量 grep 无任何 event_listener 引用（全仓 41 crate 依赖扫描唯一非误报死项；wnode rustls-pki-types 候选经核实系经 rustls::pki_types 重导出于 wnode/src/tls/config.rs:18 消费，非死）。event-listener 的真实消费方是 wbase（wbase/src/throttle.rs:13、pool/event_queue.rs:7，经 wbase Cargo.toml pool feature 的 dep:event-listener optional 声明），waof 经 wbase feature 已传递可得，直接声明为纯死条目，误导依赖图。修法：删 waof/Cargo.toml 该行（cargo remove event-listener）。
  rust：wedb/waof/Cargo.toml:22（死声明）；真实消费方 wedb/wbase/src/throttle.rs:13、wedb/wbase/src/pool/event_queue.rs:7、wedb/wbase/Cargo.toml（pool feature 的 dep:event-listener）
  c#：无对位（cargo 清单面；属一处定义原则的依赖图整洁面）

处理结果：重复拒绝
命中：task/ing/waof-dead-event-listener-dep.md（别名同题票，已认领且方案已细化，内容完全一致：删 waof/Cargo.toml:22 event-listener 死声明，走 cargo remove）。本票不重复开发。
