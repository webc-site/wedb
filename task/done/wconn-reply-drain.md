# wconn 应答消费滞留：合包应答滞留 read_buf 可致命令永挂

## 问题

wedb/wconn/src/network.rs network_loop 应答消费以「每轮必 read」驱动：

1. 内层应答循环先 stream.read 再解析 read_buf，解析循环以「队列空」为退出条件
2. 一次 read 送达多条完整应答、在途命令数少于应答数时，多余应答滞留 read_buf
3. 下一轮循环无排空动作：新命令写出后直接阻塞 stream.read 等新 socket 字节，
   已缓冲的完整应答不被认领
4. 对端把应答全部发出后不再读 socket（逐帧应答静默端点、合包形态）时，
   阻塞 read 永不返回，命令永挂
5. :254 orphan_error_reply 仅在空闲探测分支、队列空、-ERR 前导时兜底，
   覆盖不了非错误滞留与新命令到来后的场景

## 对标

garnet/libs/client/ClientSession/GarnetClientSession.cs:ProcessReplies
（TryConsumeMessages 直呼 ProcessReplies）：读事件回调内
while (readHead < bytesRead) 逐应答解析排空，每解析一条完整应答即出队一个
TaskCompletionSource 派发，一次读事件送达的全部完整应答就地消费完毕，
解析推进不受后续读事件约束。

## 改动点

只动 wedb/wconn/src/network.rs：

1. 抽出 dispatch_replies：解析循环以「read_buf 无完整应答或队列空」为退出条件，
   保留未消费字节在 read_buf 等后续读事件拼接（对标 ProcessReplies 排空语义）
2. network_loop 泵循环顶部先 dispatch_replies 排空已有完整应答，再分支：
   队列非空且队首应答不完整 → 阻塞读补齐；否则限时等新命令（空闲期
   EOF 探测读 + orphan 错误感知原样保留）
3. 不新增符号删除，无 ignore 登记需求

## 验收口径

1. 新增合包形态测试：静默假端点读第一帧后一次性写回两条应答（合包）、
   之后不再读 socket；第二条命令的应答已在 read_buf 缓冲，客户端必须完成
   （超时即滞留回归）。测试在旧实现下超时失败，新实现通过
2. ./clippy.sh 零警告（禁 allow）
3. ./test.sh 全过
4. bun ./js/check.js 无新增缺失（基线已确认零输出）

## 验证结果

1. 甄别：问题成立。C# GarnetClientSession.cs:ProcessReplies（TryConsumeMessages
   直呼）读事件内 while (readHead < bytesRead) 排空全部完整应答；旧 rust 实现
   内层循环先 stream.read 再解析、以队列空退出，合包应答数多于在途命令数时
   剩余应答滞留 read_buf，新命令写出后阻塞 read 先于解析，静默端点下永挂
2. 回归测试 network::tests::coalesced_reply_drain：静默假端点读首帧后合包写回
   +OK 与 $2\r\nv2 两条应答、此后不再读 socket；旧实现下该测试 5s 超时失败
   （实测 FAILED, finished in 5.01s），新实现通过
3. 验收：./clippy.sh 零警告（-D warnings，无 allow）；./test.sh 全过
   （1996 passed + regress 2 passed）；bun ./js/check.js 零输出无新增缺失
4. 分支 w2-wconn-drain 已合并 dev（无冲突，dev 只动 wresp/wnode/wconf）并
   fast-forward 合并回主目录；worktree 已移除、分支已删除；clippy --fix 产生的
   导入排序/行宽格式修正已单独提交（e7f1d95）
