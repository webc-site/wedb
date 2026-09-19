优先级：中
来源：next/agy.design.md 条 11。
取证基线：主仓 dev 当下代码，行号为当下实测。

问题
集合类型探测三连跳（Meta 域 → ObjectEnvelope 信封域 → String 域）在同步与异步两条装载链上
逐段手写，四个函数两两同构瀑布流：load 一对、length 一对；每对内部「三连探针 + WRONGTYPE
判定 + Degrade 短路」逐行同形，仅读原语不同（try_read_tag_sync vs read_tag_with）。
任一探测口径改动需四处同步，漂移无人拦。

取证
- wedb/wnode/src/resp/objects/object_store_utils.rs:254 fn obj_load_custom_sync
- :319 pub async fn obj_load_typed_async（本体只是 custom 的类型糖转调）
- :331 附近 pub async fn obj_load_custom_async（Meta 探测起 :338）
- :392 pub fn obj_length_sync
- :455 pub async fn obj_length_async
- sync 侧两函数（obj_load_custom_sync / obj_length_sync）与 async 侧两函数
  （obj_load_custom_async / obj_length_async）各自逐段同构；load 与 length 之间第三层同构
  （仅第 2 步取值不同：反序列化 vs count_of_blob 直读）
- C# 对标：garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs 的对象读路径
  探测逻辑一处定义，同步/异步共享同一判定骨架（CompletePending 重放同语义）

修法建议
抽统一 probe_key_domain 工具（或参数化闭包注入「信封域取值器」），三步探针与
WRONGTYPE/Degrade/Missing 判定收敛为一处定义；obj_load 与 obj_length 各自只提供取值差异，
sync/async 由读原语参数化（已有 try_read_tag_sync / read_tag_with 两个读入口可作 trait 或
枚举分派）。纯收敛，禁改探测顺序与应答字节。
