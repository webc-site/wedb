#!/usr/bin/env -S bun
import { readdir } from "node:fs/promises";
import { join } from "node:path";

const ROOT_DIR = join(import.meta.dirname, ".."),
  candidate_redis_dir_li = [
    join(ROOT_DIR, "..", "redis", "src", "commands"),
    join(ROOT_DIR, "..", "redis", "src", "cmds"),
    "/Users/z/git/db/redis/src/commands",
  ],
  REDIS_COMMANDS_DIR = candidate_redis_dir_li.find((d) => Bun.file(d).exists) ?? candidate_redis_dir_li[0],
  PARSE_DIR = join(ROOT_DIR, "cmd", "src", "parse"),
  DOC_DIR = join(ROOT_DIR, "doc");

// 1. 递归格式化 Redis 参数定义为人类可读语法
const argFormat = (arg) => {
  let res = "";
  if (arg.token && arg.name && arg.token.toLowerCase() !== arg.name.toLowerCase()) {
    res = arg.token + " " + arg.name;
  } else if (arg.token) {
    res = arg.token;
  } else {
    res = arg.name ?? "arg";
  }

  if (arg.type === "oneof" && Array.isArray(arg.arguments)) {
    const inner = arg.arguments.map(argFormat).join(" | ");
    res = "[" + inner + "]";
  } else if (arg.type === "block" && Array.isArray(arg.arguments)) {
    const inner = arg.arguments.map(argFormat).join(" ");
    res = "[" + inner + "]";
  } else if (arg.optional) {
    res = "[" + res + "]";
  }

  if (arg.multiple) {
    res += " [" + arg.name + " ...]";
  }

  return res;
};

const cmdSyntaxFormat = (cmd_name, cmd_def) => {
  if (!Array.isArray(cmd_def.arguments) || cmd_def.arguments.length === 0) {
    return cmd_name;
  }
  const args_str = cmd_def.arguments.map(argFormat).join(" ");
  return (cmd_name + " " + args_str).replaceAll(/\s+/g, " ").trim();
};

// 2. 加载所有 Redis 官方命令元数据
const redisCmdsLoad = async () => {
  const file_li = await readdir(REDIS_COMMANDS_DIR),
    redis_cmd_map = new Map();

  await Promise.all(
    file_li
      .filter((file) => file.endsWith(".json"))
      .map(async (file) => {
        try {
          const file_path = join(REDIS_COMMANDS_DIR, file),
            data = await Bun.file(file_path).json(),
            base_name = file.replace(".json", "").replaceAll("-", " ").toUpperCase();
          for (const [cmd_key, cmd_def] of Object.entries(data)) {
            let normalized_name = cmd_key.toUpperCase().replaceAll("-", " ");
            if (file.includes("-") && !normalized_name.includes(" ")) {
              normalized_name = base_name;
            }
            redis_cmd_map.set(normalized_name, {
              name: normalized_name,
              summary: cmd_def.summary ?? "",
              group: cmd_def.group ?? "generic",
              since: cmd_def.since ?? "1.0.0",
              syntax: cmdSyntaxFormat(normalized_name, cmd_def),
              arguments: cmd_def.arguments ?? [],
            });
          }
        } catch (e) {
          console.error("Failed to parse " + file + ":", e);
        }
      })
  );

  return redis_cmd_map;
};

// 3. 模块元数据与描述定义（纯中文）
const MODULE_METADATA_LI = [
  {
    id: "string",
    title: "字符串与位图",
    desc: "提供高吞吐的字符串存储、数值原子增减、位图与任意位宽位段操作，并内置支持条件增减 (INCREX)、条件删除 (DELEX)、原子 CAS/CAD 与极速哈希 (DIGEST) 等扩展命令。",
  },
  {
    id: "hash",
    title: "哈希字典",
    desc: "基于 LSM-Tree 二级索引编码的紧凑哈希表结构，支持字段级独立 TTL 过期机制 (HEXPIRE/HTTL)、原子提取删除 (HGETDEL)、原子提取延期 (HGETEX) 以及字典序前缀范围扫描 (HRANGEBYLEX)。",
  },
  {
    id: "list",
    title: "双向列表",
    desc: "支持双端队列的高速推入弹出 (LPUSH/RPOP)、阻塞弹出 (BLPOP/BRPOP)、多列表弹出 (LMPOP/BLMPOP)、范围截取 (LRANGE/LTRIM) 与跨列表原子移动 (LMOVE/BLMOVE)。",
  },
  {
    id: "set",
    title: "无序集合",
    desc: "支持高性能集合成员判定 (SISMEMBER/SMISMEMBER)、随机抽样 (SRANDMEMBER/SPOP)、增量游标迭代 (SSCAN)，以及多集合交集/并集/差集的基数计算与存储 (SINTER/SUNION/SDIFF)。",
  },
  {
    id: "zset",
    title: "有序集合",
    desc: "基于跳表与分数-成员双向复合键编码的高性能有序集合，支持按分数 (BYSCORE)、字典序 (BYLEX) 与索引范围检索，支持多集合加权聚合 (ZUNION/ZINTER/ZDIFF) 与阻塞极值弹出 (BZPOPMIN/BZMPOP)。",
  },
  {
    id: "geo",
    title: "空间地理位置",
    desc: "基于 52 位 Geohash 编码与 Haversine 大圆距离公式，支持经纬度坐标写入、位置距离与哈希值计算、以及现代矩形与半径范围搜索 (GEOSEARCH/GEOSEARCHSTORE)。",
  },
  {
    id: "hll",
    title: "基数统计",
    desc: "基于 14 位寄存器分桶与偏差修正算法的概率性基数统计引擎，支持稀疏与密集编码自适应转换，支持千万级独立元素去重统计与多键合并 (PFMERGE)。",
  },
  {
    id: "stream",
    title: "消息流与消费者组",
    desc: "支持持久化消息追加、毫秒级唯一 ID 自动递增、多流阻塞订阅 (XREAD)、消费者组与待处理条目表 PEL 管理 (XREADGROUP/XPENDING/XCLAIM/XAUTOCLAIM)，以及原子确认删除 (XACKDEL) 扩展命令。",
  },
  {
    id: "generic",
    title: "通用键与生命周期",
    desc: "管理键级别的生命周期，包含毫秒级 TTL/PTTL 过期、持久化、重命名、类型检测、通用排序 (SORT)、以及基于底层 LSM 前缀索引的高速扫描扩展命令 (SCANPREFIX)。",
  },
  {
    id: "conn_server",
    title: "连接与服务器管理",
    desc: "包含客户端连接保活 (PING)、鉴权 (AUTH)、多库切换 (SELECT)、运行时配置动态调整 (CONFIG)、集群角色查询 (ROLE)、LSM 物理压缩 (COMPACT) 与实例统计信息 (INFO)。",
  },
  {
    id: "pubsub",
    title: "发布订阅",
    desc: "支持经典频道与模式匹配广播 (PUBLISH/SUBSCRIBE/PSUBSCRIBE)、分片发布订阅 (SPUBLISH/SSUBSCRIBE)，以及单网络往返多频道批量发布扩展命令 (MPUBLISH)。",
  },
  {
    id: "txn",
    title: "事务与批量引擎",
    desc: "提供乐观锁事务块 (MULTI/EXEC/WATCH) 以及单往返原子多操作批量写入执行引擎 (BATCH) 与跨分片分布式事务扩展命令 (TXN)。",
  },
  {
    id: "cluster_raft",
    title: "分布式集群与共识",
    desc: "完全兼容 Redis Cluster 拓扑发现协议 (CLUSTER NODES/SLOTS/SHARDS)，底层深度集成 Raft 强一致性共识状态机扩展命令 (RAFT LEADER/CLUSTER_INFO/MEMBERSHIP)。",
  },
  {
    id: "bloom",
    title: "布隆与布谷鸟过滤器",
    desc: "提供可伸缩链式布隆过滤器 (Bloom Filter) 与支持动态删除的布谷鸟过滤器 (Cuckoo Filter) 扩展命令，以极小内存开销实现超大规模数据集的高速存在性过滤。",
  },
  {
    id: "json",
    title: "JSON 文档引擎",
    desc: "提供符合 RFC 7396 / RFC 6901 标准的原生 JSON 文档存储扩展命令，支持复杂的 JSONPath 表达式查询、局部路径就地修改、数字乘加与类型转换。",
  },
  {
    id: "search",
    title: "全文与向量检索",
    desc: "支持对 Hash 和 JSON 文档建立倒排索引、数值与标签过滤、同义词解析、聚合分析管道，以及基于 HNSW 图索引的高维向量近邻相似度检索扩展命令。",
  },
  {
    id: "timeseries",
    title: "高性能时序引擎",
    desc: "专为时序数据设计的高吞吐分块存储，内置 Gorilla 双重增量压缩算法、多时间序列标签过滤、滑动窗口聚合与自动降采样连续聚合规则扩展命令。",
  },
  {
    id: "tdigest",
    title: "分位数统计草图",
    desc: "基于质心聚类与缩放函数的流式高精度分位数估计扩展命令，支持极值准确保留、百分位数估计、累积分布概率 (CDF)、排位估计与草图合并。",
  },
  {
    id: "sortedint",
    title: "紧凑有序整型集合",
    desc: "专为千万级 64 位无符号大整数（ID 集合）深度优化的紧凑去重与排序扩展命令，支持按值开闭区间极速分页查询与存在性统计。",
  },
  {
    id: "namespace",
    title: "多租户命名空间隔离",
    desc: "原生内置企业级多租户虚拟化隔离扩展命令，支持为不同租户分配独立命名空间、Token 鉴权、配额控制与整租户数据毫秒级原子销毁。",
  },
];

const FILE_TO_MODULE = {
  bloom: "bloom",
  cluster: "cluster_raft",
  conn: "conn_server",
  geo: "geo",
  hash: "hash",
  hll: "hll",
  json: "json",
  key: "generic",
  list: "list",
  pubsub: "pubsub",
  script: "conn_server",
  search: "search",
  set: "set",
  sortedint: "sortedint",
  stream: "stream",
  string: "string",
  tdigest: "tdigest",
  timeseries: "timeseries",
  txn: "txn",
  zset: "zset",
};

const EXTENSION_NAMES = new Set([
  "INCREX", "DELEX", "CAS", "CAD", "DIGEST",
  "HGETDEL", "HGETEX", "HRANGEBYLEX",
  "XACKDEL", "XDELEX", "XNACK",
  "SCANPREFIX", "KMETADATA", "MOVEX", "COMPACT",
  "MPUBLISH", "BATCH", "TXN",
]);

const COMPOUND_COMMAND_PARENTS = new Set([
  "CLUSTER", "CLIENT", "NAMESPACE", "CONFIG", "XGROUP", "SCRIPT", "FUNCTION", "PUBSUB", "XINFO",
]);

const REDIS_GROUP_TITLES = {
  server: "服务运维与权限",
  sentinel: "哨兵系统",
  scripting: "脚本与函数库",
  cluster: "集群运维与槽位迁移",
  connection: "连接追踪与控制",
  generic: "通用键运维",
  stream: "流消息运维",
  pubsub: "发布订阅控制",
  transactions: "事务控制",
  string: "字符串扩展",
  hash: "哈希扩展",
  list: "列表扩展",
  set: "集合扩展",
  sorted_set: "有序集合扩展",
  geo: "空间地理扩展",
  hyperloglog: "基数统计扩展",
  bitmap: "位图扩展",
  array: "数组扩展",
};

const EXTENSION_SYNTAX_FALLBACK = {
  "INCREX": "INCREX key [INCRBY int | INCRBYFLOAT float] [SATURATE] [LBOUND min] [UBOUND max] [EX sec | PX ms]",
  "DELEX": "DELEX key [IFEQ val | IFNE val]",
  "CAS": "CAS key oldval newval [EX sec | PX ms]",
  "CAD": "CAD key oldval",
  "DIGEST": "DIGEST key",
  "HGETDEL": "HGETDEL key FIELDS numfields field [field ...]",
  "HGETEX": "HGETEX key [EX sec | PX ms] FIELDS numfields field [field ...]",
  "HRANGEBYLEX": "HRANGEBYLEX key min max [LIMIT offset count]",
  "XACKDEL": "XACKDEL key group id [id ...]",
  "XDELEX": "XDELEX key [MAXLEN maxlen] id [id ...]",
  "XNACK": "XNACK key group id [id ...]",
  "SCANPREFIX": "SCANPREFIX prefix cursor [COUNT count]",
  "KMETADATA": "KMETADATA key",
  "MOVEX": "MOVEX key src_db dst_db",
  "COMPACT": "COMPACT",
  "MPUBLISH": "MPUBLISH channel message [channel message ...]",
  "BATCH": "BATCH <op> <args...> [OP ...]",
  "TXN": "TXN",
  "NAMESPACE ADD": "NAMESPACE ADD <name> <token>",
  "NAMESPACE SET": "NAMESPACE SET <name> <token>",
  "NAMESPACE DEL": "NAMESPACE DEL <name>",
  "NAMESPACE GET": "NAMESPACE GET <name | *>",
  "NAMESPACE CURRENT": "NAMESPACE CURRENT",
  "RAFT LEADER": "RAFT LEADER",
  "RAFT CLUSTER_INFO": "RAFT CLUSTER_INFO",
  "RAFT ADD_LEARNER": "RAFT ADD_LEARNER node_id addr",
  "RAFT CHANGE_MEMBERSHIP": "RAFT CHANGE_MEMBERSHIP [node_id ...]",
  "RAFT JOIN": "RAFT JOIN node_id addr",
  "RAFT LEAVE": "RAFT LEAVE node_id",
  "RAFT MEMBERS": "RAFT MEMBERS",
  "RAFT SNAPSHOT": "RAFT SNAPSHOT",
  "RAFT SNAPSHOT_STATUS": "RAFT SNAPSHOT_STATUS",
  "RAFT PURGE": "RAFT PURGE [upto]",
  "RAFT HEALTH": "RAFT HEALTH",
  "RAFT METRICS": "RAFT METRICS",
  "RAFT STATUS": "RAFT STATUS",
  "SIADD": "SIADD key id [id ...]",
  "SIREM": "SIREM key id [id ...]",
  "SICARD": "SICARD key",
  "SIEXISTS": "SIEXISTS key id [id ...]",
  "SIMCOUNT": "SIMCOUNT key id [id ...]",
  "SIRANGE": "SIRANGE key offset count",
  "SIREVRANGE": "SIREVRANGE key offset count",
  "SIRANGEBYVALUE": "SIRANGEBYVALUE key min max [LIMIT offset count]",
  "SIREVRANGEBYVALUE": "SIREVRANGEBYVALUE key max min [LIMIT offset count]",
  "LMOVEM": "LMOVEM source destination <LEFT | RIGHT> <LEFT | RIGHT> count",
  "BLMOVEM": "BLMOVEM source destination <LEFT | RIGHT> <LEFT | RIGHT> count timeout",
  "FT.SEARCHSQL": "FT.SEARCHSQL index query",
  "FT.EXPLAINSQL": "FT.EXPLAINSQL index query",
  "FT._LIST": "FT._LIST",
  "FT.TAGVALS": "FT.TAGVALS index field_name",
  "JSON.ARRTRIM": "JSON.ARRTRIM key path start stop",
  "JSON.MSET": "JSON.MSET key path value [key path value ...]",
  "JSON.RESP": "JSON.RESP key [path]",
  "JSON.INFO": "JSON.INFO key",
};

// 4. 基于 Rust 源码代码结构直接解析
const wedbCmdScanFromRustSource = async (redis_cmd_map) => {
  const mod_rs_path = join(PARSE_DIR, "mod.rs"),
    mod_rs_content = await Bun.file(mod_rs_path).text(),
    macro_match = mod_rs_content.match(/dispatch_commands!\s*\{([\s\S]*?)\}/),
    scanned_li = [],
    seen_set = new Set();

  if (macro_match) {
    const body = macro_match[1],
      section_li = body.split(/,\s*(?=[a-z_]+:)/);

    for (const sec of section_li) {
      const colon_idx = sec.indexOf(":");
      if (colon_idx === -1) continue;
      const mod_name = sec.slice(0, colon_idx).trim(),
        cmds_part = sec.slice(colon_idx + 1),
        mapped_mod = FILE_TO_MODULE[mod_name] || mod_name,
        cmd_token_li = cmds_part
          .split("|")
          .map((c) => c.trim().replace(/,/g, "").toLowerCase())
          .filter((c) => c.length > 0);

      for (const token of cmd_token_li) {
        if (token.startsWith("_") && token !== "_list") continue;
        let name = token.toUpperCase();
        if (name === "CMD") name = "COMMAND";

        if (COMPOUND_COMMAND_PARENTS.has(name)) {
          const file_path = join(PARSE_DIR, mod_name + ".rs");
          try {
            const content = await Bun.file(file_path).text(),
              line_li = content.split("\n");
            let inside_compound = false;

            for (const line of line_li) {
              const trimmed = line.trim();
              if (trimmed.includes('"' + token + '"') || trimmed.includes('"' + token.toLowerCase() + '"')) {
                inside_compound = true;
                continue;
              }
              if (inside_compound && (trimmed.startsWith('        "') || trimmed.startsWith('            "'))) {
                const sub_match = trimmed.match(/^"([a-z0-9_.-]+)"/i);
                if (sub_match) {
                  const sub_cmd = sub_match[1].toUpperCase();
                  if (sub_cmd.startsWith("_") || sub_cmd === "HELP") continue;
                  const full_cmd = name + " " + sub_cmd;
                  if (!seen_set.has(full_cmd)) {
                    seen_set.add(full_cmd);
                    const r_cmd = redis_cmd_map.get(full_cmd),
                      syntax = r_cmd?.syntax || EXTENSION_SYNTAX_FALLBACK[full_cmd] || (full_cmd + " [args ...]"),
                      is_ext = full_cmd.includes(".")
                        || EXTENSION_NAMES.has(full_cmd)
                        || full_cmd.startsWith("RAFT ")
                        || full_cmd.startsWith("NAMESPACE ")
                        || full_cmd.startsWith("SI");

                    let target_mod = mapped_mod;
                    if (full_cmd.startsWith("NAMESPACE")) target_mod = "namespace";

                    scanned_li.push({
                      name: full_cmd,
                      mod: target_mod,
                      syntax,
                      isExt: is_ext,
                    });
                  }
                }
              }
            }
          } catch {}
          continue;
        }

        if (!seen_set.has(name)) {
          seen_set.add(name);
          const r_cmd = redis_cmd_map.get(name),
            syntax = r_cmd?.syntax || EXTENSION_SYNTAX_FALLBACK[name] || (name + " [args ...]"),
            is_ext = name.includes(".")
              || EXTENSION_NAMES.has(name)
              || name.startsWith("RAFT ")
              || name.startsWith("NAMESPACE ")
              || name.startsWith("SI");

          scanned_li.push({
            name,
            mod: mapped_mod,
            syntax,
            isExt: is_ext,
          });
        }
      }
    }
  }

  const extra_raft_cmd_li = [
    "RAFT LEADER", "RAFT CLUSTER_INFO", "RAFT ADD_LEARNER", "RAFT CHANGE_MEMBERSHIP",
    "RAFT JOIN", "RAFT LEAVE", "RAFT MEMBERS", "RAFT SNAPSHOT", "RAFT SNAPSHOT_STATUS",
    "RAFT PURGE", "RAFT HEALTH", "RAFT METRICS", "RAFT STATUS",
  ];
  for (const r_name of extra_raft_cmd_li) {
    if (!seen_set.has(r_name)) {
      seen_set.add(r_name);
      scanned_li.push({
        name: r_name,
        mod: "cluster_raft",
        syntax: EXTENSION_SYNTAX_FALLBACK[r_name] || r_name,
        isExt: true,
      });
    }
  }

  return scanned_li;
};

// 5. 生成结构化对比数据
const dataGenerate = async () => {
  const redis_cmd_map = await redisCmdsLoad(),
    wedb_cmd_li = await wedbCmdScanFromRustSource(redis_cmd_map),
    wedb_cmd_map = new Map();

  for (const item of wedb_cmd_li) {
    wedb_cmd_map.set(item.name.toUpperCase(), item);
  }

  const module_group_li = MODULE_METADATA_LI.map((mod) => {
    const cmd_li = wedb_cmd_li
      .filter((item) => item.mod === mod.id)
      .map((item) => ({
        name: item.name,
        syntax: item.syntax,
        status: item.isExt ? "扩展命令" : "支持",
      }))
      .sort((a, b) => a.name.localeCompare(b.name));

    return {
      id: mod.id,
      title: mod.title,
      description: mod.desc,
      count: cmd_li.length,
      commands: cmd_li,
    };
  });

  const unsupported_map = new Map();
  let total_unsupported_count = 0;

  for (const [r_name, r_def] of redis_cmd_map.entries()) {
    if (!wedb_cmd_map.has(r_name)) {
      ++total_unsupported_count;
      const grp = r_def.group || "generic";
      if (!unsupported_map.has(grp)) {
        unsupported_map.set(grp, []);
      }
      unsupported_map.get(grp).push({
        name: r_name,
        group: grp,
        syntax: r_def.syntax,
        status: "不支持",
      });
    }
  }

  const unsupported_group_li = [];
  for (const [grp, cmd_li] of unsupported_map.entries()) {
    cmd_li.sort((a, b) => a.name.localeCompare(b.name));
    unsupported_group_li.push({
      group: grp,
      title: REDIS_GROUP_TITLES[grp] || (grp + " 模块"),
      count: cmd_li.length,
      commands: cmd_li,
    });
  }
  unsupported_group_li.sort((a, b) => b.count - a.count || a.group.localeCompare(b.group));

  const total_supported = wedb_cmd_li.length,
    compatible_count = wedb_cmd_li.filter((c) => !c.isExt).length,
    extension_count = wedb_cmd_li.filter((c) => c.isExt).length;

  return {
    meta: {
      generated_at: new Date().toISOString(),
      source_scan_path: "cmd/src/parse/*.rs",
      total_wedb_commands: total_supported,
      compatible_count,
      extension_count,
      unsupported_redis_count: total_unsupported_count,
      total_redis_commands: redis_cmd_map.size,
    },
    modules: module_group_li,
    unsupported_redis_commands: {
      title: "不支持的命令",
      description: "以下命令为 Redis 官方定义但 WeDB 当前尚未支持的命令：",
      total_count: total_unsupported_count,
      groups: unsupported_group_li,
    },
  };
};

// 6. 纯净格式化 YAML 输出
const yamlDump = (obj, indent = 0) => {
  const pad = " ".repeat(indent);
  if (Array.isArray(obj)) {
    if (obj.length === 0) return "[]\n";
    return obj.map((item) => {
      if (typeof item === "object" && item !== null) {
        const lines = yamlDump(item, indent + 2).trimStart();
        return pad + "- " + lines;
      }
      return pad + "- " + JSON.stringify(item) + "\n";
    }).join("");
  } else if (typeof obj === "object" && obj !== null) {
    return Object.entries(obj).map(([k, v]) => {
      if (typeof v === "object" && v !== null) {
        return pad + k + ":\n" + yamlDump(v, indent + 2);
      }
      const val_str = typeof v === "string" && (v.includes(":") || v.includes("\n") || v.includes('"') || v.includes("#"))
        ? JSON.stringify(v)
        : String(v);
      return pad + k + ": " + val_str + "\n";
    }).join("");
  }
  return pad + JSON.stringify(obj) + "\n";
};

// 7. 生成纯净优雅的 Markdown 矩阵文档
const markdownGenerate = (data) => {
  const { meta, modules, unsupported_redis_commands } = data;

  let md = "# WeDB 命令规格与兼容性对照表\n\n" +
    "> **生成时间**：" + meta.generated_at + "  \n" +
    "> **命令总数**：`" + meta.total_wedb_commands + "`（标准命令：`" + meta.compatible_count + "`，扩展命令：`" + meta.extension_count + "`）  \n" +
    "> **不支持命令**：`" + meta.unsupported_redis_count + "`（置于文末分类展示）\n\n" +
    "---\n\n" +
    "## 目录\n\n" +
    "### 一、支持与扩展命令\n";

  modules.forEach((mod, idx) => {
    md += "- [" + (idx + 1) + ". " + mod.title + "](#" + (idx + 1) + "-" + mod.title.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fa5]/g, "") + ")\n";
  });

  md += "\n### 二、不支持的命令\n";
  unsupported_redis_commands.groups.forEach((grp, idx) => {
    md += "- [末尾." + (idx + 1) + " " + grp.title + " (" + grp.count + ")](#末尾" + (idx + 1) + "-" + grp.title.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fa5]/g, "") + ")\n";
  });

  md += "\n---\n\n";

  modules.forEach((mod, idx) => {
    md += "## " + (idx + 1) + ". " + mod.title + "\n\n" +
      mod.description + "\n\n" +
      "| 命令 | 语法 | 状态 |\n" +
      "| :--- | :--- | :---: |\n";

    for (const cmd of mod.commands) {
      const status_badge = cmd.status === "扩展命令" ? "🌟" : "✅";
      md += "| `" + cmd.name + "` | `" + cmd.syntax.replaceAll("|", "\\|") + "` | " + status_badge + " |\n";
    }
    md += "\n---\n\n";
  });

  md += "## 末尾：" + unsupported_redis_commands.title + "\n\n" +
    unsupported_redis_commands.description + "\n\n";

  unsupported_redis_commands.groups.forEach((grp, idx) => {
    md += "### 末尾." + (idx + 1) + " " + grp.title + "\n\n" +
      "| 命令 | 语法 | 状态 |\n" +
      "| :--- | :--- | :---: |\n";

    for (const cmd of grp.commands) {
      md += "| `" + cmd.name + "` | `" + cmd.syntax.replaceAll("|", "\\|") + "` |  |\n";
    }
    md += "\n";
  });

  return md;
};

// 8. 主执行入口
const main = async () => {
  console.log("正在解析 Rust 源码语法分支 (cmd/src/parse/*.rs) 并生成纯中文对照表...");
  const data = await dataGenerate(),
    yml_content = yamlDump(data),
    cmd_yml_path = join(DOC_DIR, "cmd.yml");

  await Bun.write(cmd_yml_path, yml_content);
  await Bun.write(join(ROOT_DIR, "cmd.yml"), yml_content);
  console.log("成功写入模块化 YAML 文档配置: " + cmd_yml_path);

  const md_content = markdownGenerate(data),
    cmd_md_path = join(DOC_DIR, "cmd.md");
  await Bun.write(cmd_md_path, md_content);
  console.log("成功写入纯净 Markdown 对照表: " + cmd_md_path);

  console.log("\n统计摘要:");
  console.log("- 命令总数: " + data.meta.total_wedb_commands);
  console.log("  * 标准命令: " + data.meta.compatible_count);
  console.log("  * 扩展命令: " + data.meta.extension_count);
  console.log("- 不支持的命令: " + data.meta.unsupported_redis_count + "（按 " + data.unsupported_redis_commands.groups.length + " 个模块划分）");
};

export {
  dataGenerate,
  markdownGenerate,
  yamlDump,
  main,
};

export default main;

if (import.meta.main) {
  main().catch((err) => {
    console.error("执行失败:", err);
    process.exit(1);
  });
}
