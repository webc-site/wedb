fsm visit_used 空表哨兵未设防：max_block 为 NO_BLOCK 时 0..u32::MAX+1 调试构建溢出 panic（is_free 同哨兵已设防，单点漏挂）

问题分析：
1 Garnet 契约对齐：C# 无对位物——本 FSM 系 rust 自建面（原生 diskann-rs 移植承载，C# 迁移导出走 DiskANNService 原生枚举路径，无空表哨兵形态），按 review.md 板块 4.1 边界防御维度收口。
2 工程现状确证：wedb/wvector/src/fsm.rs:555-559 visit_used 头部读 let max_block = self.id_minter.read().max_block; 后 for block_id in 0..max_block + 1 无哨兵早退。空表哨兵 NO_BLOCK = u32::MAX（fsm.rs:169，构造初值 :186）未消费即入 max_block + 1——u32 加法在 overflow-checks 开启构建（dev/test/bench）溢出 panic，release 依赖回绕为 0 才侥幸得空循环。同文件同类读点均已设防：is_free（fsm.rs:341 if max_block == NO_BLOCK { return Ok(true); }，:338-343）、:443 短路先判 NO_BLOCK 再算 +1，唯 visit_used 漏挂。
3 逻辑危害确证：零元素向量集（建籍后未 VADD 即迁移/导出）触发链：vector_manager_migration.rs:100-124 export_migration_elements → service.rs:601-617 enumerate_elements → fsm.visit_used → 调试构建 panic。危害限调试与测试构建面（release 语义侥幸正确）；训练臂调用点 dynamic_quant.rs:219 有 count 门掩蔽，现锁全绿。

涉及代码：
rust 文件与函数：
wedb/wvector/src/fsm.rs:visit_used（:550-559 无哨兵环）、NO_BLOCK（:169/:186）、is_free 设防先例（:338-343）、:443 短路形

对应 c# 文件与函数：
N.A.（rust 自建 FSM 面，C# 迁移导出走 DiskANNService 原生枚举，无对位；板块 4.1 边界防御维度收口）

精炼执行方案：
1 visit_used 头部补 if max_block == NO_BLOCK { return Ok(()); } 早退臂，与 is_free 同哨兵单点同形
2 锁测：零元素向量集（建籍未 VADD）export/枚举路径 debug 构建跑通不 panic，断言空集导出语义
