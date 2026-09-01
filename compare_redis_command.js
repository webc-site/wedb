#!/usr/bin/env bun

import fs from 'fs';
import path from 'path';

const redisCommandsDir = path.resolve('../redis/src/cmds');
const cmdDir = path.resolve('cmd/src');
const clusterDir = path.resolve('cluster/src/redis');
const standaloneDir = path.resolve('standalone/src');

if (!fs.existsSync(redisCommandsDir)) {
  console.error(`Redis cmds directory not found at: ${redisCommandsDir}`);
  process.exit(1);
}

// 1. 读取 ../redis/src/cmds 下所有命令 JSON
const files = fs.readdirSync(redisCommandsDir).filter(f => f.endsWith('.json'));
const redisCommands = new Map();

for (const file of files) {
  const filePath = path.join(redisCommandsDir, file);
  try {
    const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    for (const [cmdName, cmdDef] of Object.entries(content)) {
      const container = cmdDef.container ? cmdDef.container.toUpperCase() : null;
      const baseName = cmdName.toUpperCase();
      const fullName = container ? `${container} ${baseName}` : baseName;

      redisCommands.set(fullName, {
        name: fullName,
        baseName,
        container,
        group: cmdDef.group || 'generic',
        summary: cmdDef.summary || '',
        since: cmdDef.since || '',
        arguments: cmdDef.arguments || [],
        subcmds: cmdDef.subcmds || {},
        file,
      });
    }
  } catch (err) {
    // 忽略解析错误
  }
}

// 2. 读取 WeDb Rust 源码中的所有指令与子指令匹配
function readAllRsFiles(dir) {
  if (!fs.existsSync(dir)) return '';
  let res = '';
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      res += readAllRsFiles(full);
    } else if (entry.name.endsWith('.rs')) {
      res += fs.readFileSync(full, 'utf8') + '\n';
    }
  }
  return res;
}

const allRsContent = [
  readAllRsFiles(cmdDir),
  readAllRsFiles(clusterDir),
  readAllRsFiles(standaloneDir)
].join('\n');

const parsedTokens = new Set();

// 提取所有字符串字面量（包括 match "foo" | "bar" => 分支）
const strLiteralRegex = /"([a-zA-Z0-9._-]+)"/g;
let m;
while ((m = strLiteralRegex.exec(allRsContent)) !== null) {
  parsedTokens.add(m[1].toUpperCase());
}

// 提取 RedisCommand / Cmd 枚举变体与标识符
const enumVariantRegex = /\b([A-Z][A-Za-z0-9_]+)\b/g;
while ((m = enumVariantRegex.exec(allRsContent)) !== null) {
  parsedTokens.add(m[1].toUpperCase());
  // 支持驼峰转空格：XInfoStream -> XINFO STREAM
  const splitWords = m[1].replace(/([a-z])([A-Z])/g, '$1 $2').toUpperCase();
  parsedTokens.add(splitWords);
}

// WeDb 中已实现的底层数据结构
const supportedGroups = {
  string: ['String', 'Bitmap', 'Bit'],
  hash: ['Hash'],
  list: ['List'],
  set: ['Set'],
  'sorted-set': ['ZSet', 'Sorted Set'],
  bitmap: ['Bitmap'],
  stream: ['Stream'],
  generic: ['Keys', 'Generic'],
  server: ['Server'],
  connection: ['Connection'],
  transactions: ['Transactions'],
  pubsub: ['PubSub'],
  hyperloglog: ['HyperLogLog'],
  geo: ['Geo'],
  timeseries: ['TimeSeries'],
  bloom: ['Bloom'],
  cuckoo: ['Cuckoo'],
  tdigest: ['TDigest'],
  json: ['JSON']
};

// 缺乏对应底层模块或原生集群拓扑管理的数据结构/指令（如 Lua 虚拟机、原生 Cluster 槽迁移、Sentinel、Array 结构等）
const outOfScopeGroups = new Set(['scripting', 'sentinel', 'cluster', 'array']);
const outOfScopeCommands = new Set([
  'HIMPORT', 'HIMPORT PREPARE', 'HIMPORT DISCARDALL',
  'MIGRATE', 'WAITAOF', 'PFDEBUG', 'XCFGSET', 'CACHING',
  'CLIENT CACHING'
]);

const implemented = [];
const missingWithExistingStructure = [];
const missingNoStructure = [];

for (const [name, info] of redisCommands.entries()) {
  const parts = name.split(/[\s|-]+/);
  const normalizedName = name.replace(/[-|]/g, ' ');
  const dottedName = name.replace(/[\s|-]+/g, '.');
  const compactName = name.replace(/[\s|-]+/g, '');
  const dashName = name.replace(/\s+/g, '-');

  const isImplemented =
    parsedTokens.has(name) ||
    parsedTokens.has(normalizedName) ||
    parsedTokens.has(dottedName) ||
    parsedTokens.has(compactName) ||
    parsedTokens.has(dashName) ||
    parsedTokens.has(info.baseName) ||
    (info.container && parsedTokens.has(info.container) && parsedTokens.has(info.baseName)) ||
    parts.every(p => parsedTokens.has(p));

  if (isImplemented && !outOfScopeCommands.has(name)) {
    implemented.push(info);
  } else if (outOfScopeGroups.has(info.group) || outOfScopeCommands.has(name)) {
    missingNoStructure.push(info);
  } else if (supportedGroups[info.group]) {
    missingWithExistingStructure.push(info);
  } else {
    missingNoStructure.push(info);
  }
}

console.log(`\n======================================================`);
console.log(`          REDIS COMMANDS AUDIT & COMPARISON           `);
console.log(`======================================================`);
console.log(`Total Redis Commands in redis/src/cmds: ${redisCommands.size}`);
console.log(`✅ Implemented in WeDb: ${implemented.length}`);
console.log(`⚠️  Missing (with existing data structure in WeDb): ${missingWithExistingStructure.length}`);
console.log(`📋 Missing (no data structure / out of scope -> todo.md): ${missingNoStructure.length}\n`);

if (missingWithExistingStructure.length > 0) {
  console.log(`\n======================================================`);
  console.log(`  MISSING COMMANDS WITH EXISTING DATA STRUCTURES (ACTIONABLE)`);
  console.log(`======================================================`);
  const groupedMissing = {};
  for (const cmd of missingWithExistingStructure) {
    if (!groupedMissing[cmd.group]) groupedMissing[cmd.group] = [];
    groupedMissing[cmd.group].push(cmd);
  }

  for (const [group, cmds] of Object.entries(groupedMissing)) {
    console.log(`\n📁 Group [${group}] (${cmds.length} cmds):`);
    for (const c of cmds) {
      console.log(`  - ${c.name.padEnd(25)}: ${c.summary} (since: ${c.since || 'unknown'})`);
    }
  }
}

console.log(`\n======================================================`);
console.log(`     MISSING COMMANDS WITHOUT DATA STRUCTURE (TODO.MD)`);
console.log(`======================================================`);
const groupedNoStruct = {};
for (const cmd of missingNoStructure) {
  if (!groupedNoStruct[cmd.group]) groupedNoStruct[cmd.group] = [];
  groupedNoStruct[cmd.group].push(cmd);
}

for (const [group, cmds] of Object.entries(groupedNoStruct)) {
  console.log(`\n📁 Group [${group}] (${cmds.length} cmds):`);
  for (const c of cmds) {
    console.log(`  - ${c.name.padEnd(25)}: ${c.summary} (since: ${c.since || 'unknown'})`);
  }
}

// 写入 todo.md
const todoLines = [
  '# WeDb 待支持指令与模块清单 (TODO)',
  '',
  '以下指令因 WeDb 暂未实现对应的底层数据结构或高级运行时（如 Lua/Function 脚本引擎、原生 Redis Cluster 槽管理、Sentinel 哨兵架构、Array 数据类型等），已收录至此清单供后续规划支持。',
  ''
];

for (const [group, cmds] of Object.entries(groupedNoStruct)) {
  todoLines.push(`## ${group.toUpperCase()} 模块 (${cmds.length} 指令)`);
  for (const c of cmds) {
    todoLines.push(`- **\`${c.name}\`**: ${c.summary} *(since: ${c.since || 'N/A'})*`);
  }
  todoLines.push('');
}

fs.writeFileSync('todo.md', todoLines.join('\n'));
console.log(`\n✅ Generated todo.md successfully.`);
