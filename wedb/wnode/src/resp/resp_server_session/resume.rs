//! 慢路径续跑尾参编解码族（exec 降级快照追加的尾参 wire 格式单源）。
//!
//! 三族共用「模式字节 + 定宽 LE 字段 + 16 字节域锚段」线形，编解码统一走
//! [`TailWriter`] / [`TailReader`] 游标（线格式逐字节兼容是协议硬约束，
//! 编出的字节序列与旧手写实现逐字节一致——主从复制兼容）。

/// 续跑尾参域锚：`(vns, vdb)` 物理域对，与 wkv `StoreSession::virtual_domain`
/// 同源判据（记录键物理前缀唯一取点的标量投影），非第二套域身份。
/// 换号族（FLUSHDB / FLUSHNS / SWAPDB 的 bump_generation）改指即不等，
/// 续跑尾参携此刻度同点捕获的锚，重放臂与慢臂当前解析域现比——跨域即判
/// 刻度属死域，禁落新域（§96/§99/§118 域钉族向命令降级续跑尾参的推广，
/// 票 wnode-set-keepttl-resume-tail-no-domain-anchor-ghost-ttl-across-swap）
pub type DomainAnchor = (u64, u64);

/// 续跑腿载荷：绝对过期刻度 + 随刻度同点捕获的物理域锚
#[derive(Debug, Copy, PartialEq, Clone)]
pub struct TtlLeg {
  /// 绝对过期刻度（.NET Ticks）
  pub ticks: i64,
  /// 刻度读出 / 待投时所处的物理域
  pub domain: DomainAnchor,
}

/// 尾参线格式写游标：模式字节 + 定宽 LE 字段 + 域锚段的单源写面
///（MSETNX / TTL / ETag 三族续跑尾参共用，杜绝线形手写两遍）
struct TailWriter {
  tail: Vec<u8>,
}

impl TailWriter {
  /// 按尾参线长恰容量预分配
  fn with_len(len: usize) -> Self {
    Self {
      tail: Vec::with_capacity(len),
    }
  }

  /// 模式字节
  fn push_mode(&mut self, mode: u8) {
    self.tail.push(mode);
  }

  /// 定宽 8 字节 LE 字段（i64 位模式经 `as u64` 直投，位保真）
  fn push_le(&mut self, field: u64) {
    self.tail.extend_from_slice(&field.to_le_bytes());
  }

  /// 域锚段（16 字节：vns + vdb 两枚 8 字节 LE）
  fn push_domain(&mut self, domain: DomainAnchor) {
    self.push_le(domain.0);
    self.push_le(domain.1);
  }
}

/// 尾参线格式读游标：[`TailWriter`] 的对偶读面
struct TailReader<'a> {
  tail: &'a [u8],
  pos: usize,
}

impl<'a> TailReader<'a> {
  /// 形态门：线长精确匹配才开读（长度不符返回 None，由调用方落兜底臂）
  fn new(tail: Option<&'a [u8]>, len: usize) -> Option<Self> {
    let tail = tail?;
    (tail.len() == len).then_some(Self { tail, pos: 0 })
  }

  /// 模式字节
  fn next_mode(&mut self) -> u8 {
    let mode = self.tail[self.pos];
    self.pos += 1;
    mode
  }

  /// 定宽 8 字节 LE 字段
  fn next_le(&mut self) -> u64 {
    let mut buf = [0u8; 8];
    let end = self.pos + 8;
    buf.copy_from_slice(&self.tail[self.pos..end]);
    self.pos = end;
    u64::from_le_bytes(buf)
  }

  /// 域锚段
  fn next_domain(&mut self) -> DomainAnchor {
    (self.next_le(), self.next_le())
  }
}

/// MSETNX 慢路径承接模式（快路径降级时置位，exec 降级快照尾参承载，
/// 慢路径消费后复位；尾参编码见 [`MsetnxResume::tail_byte`]）
#[derive(Debug, Copy, PartialEq, Default, Clone)]
pub enum MsetnxResume {
  /// 无降级 / 判定段降级：慢路径全量重判（三域存活裁决 + 写入一体重放）
  #[default]
  Replay,
  /// 写入段异步闭环信号降级（环形页翻转）：NX 判定已整体通过、已写键
  /// 保持，慢路径跳过判定续写全部键值回 :1（upsert 同值幂等）
  Continue,
  /// 回滚存在删除降级残留（回滚删除遇环形页翻转被记录）：慢路径持窗
  /// 条件回滚收尾——仅删内容即本命令所写的键（禁盲删吞并发已确认写，
  /// 票 zcode-r37-lockfix 发现 B）后回 :0
  Rollback,
}

impl MsetnxResume {
  /// 模式字节单源（编码 / 逆解析共用判据；b"0"/b"1"/b"r"）
  fn mode_byte(self) -> u8 {
    match self {
      Self::Replay => b'0',
      Self::Continue => b'1',
      Self::Rollback => b'r',
    }
  }

  /// 快照尾参编码（b"0"/b"1"/b"r"；沿 MSETNX resume 尾参先例的单字节
  /// 模式标记，慢路径臂 [`MsetnxResume::from_tail`] 逆解析）
  pub fn tail_byte(self) -> u8 {
    self.mode_byte()
  }

  /// 快照尾参逆解析（未知字节按 Replay 兜底：判定段全量重判语义最保守）
  pub fn from_tail(tail: Option<&[u8]>) -> Self {
    TailReader::new(tail, 1)
      .map(|mut r| match r.next_mode() {
        b'1' => Self::Continue,
        b'r' => Self::Rollback,
        _ => Self::Replay,
      })
      .unwrap_or_default()
  }
}

/// RESTORE / SET 条件写族慢路径承接标记（票 wnode-nx-conditional-ttl-degrade-replay-selfhit：
/// 快臂「值已同步提交」后 put_ttl_sync 遭环形页翻转降级时置 Pending，exec 降级快照
/// 尾参承载，慢臂消费后跳过整命令重放、仅窗内补投 TTL 出成功帧）。
/// 杜绝重放自碰本命令已提交值——RESTORE 误回 BUSYKEY、SET NX 误回 nil、
/// SET KEEPTTL 丢 TTL 三形；对标 C# 值与过期内嵌单次 CAS 原子一体落库、
/// 无「值已落、过期未落」中间态（KeyAdminCommands.cs:105/:109、BasicCommands.cs:786）
#[derive(Debug, Copy, PartialEq, Default, Clone)]
pub enum TtlResume {
  /// 无「值已提交」降级（未降级 / 提交前降级）：慢路径整命令全量重放（既有语义）
  #[default]
  Full,
  /// 值已同步提交、TTL 待投（绝对过期刻度随尾参携带，KEEPTTL 形为回填的旧值刻度）
  Pending(TtlLeg),
  /// 快臂已读旧 TTL、值写遇页翻转降级：慢路径重放写值并按此刻度回填 TTL，
  /// 杜绝重读已被快臂 TTL 腿清退的墓碑致静默丢 TTL（跨换号域比对失配时
  /// 弃刻度改现读当前域 ttl_of，回到 C# 线性化终态新键无 TTL）
  KeepTtl(TtlLeg),
  /// GET 回旧值形值已同步提交、应答已由快臂成帧保留（旧值已覆写不可复原，
  /// 禁整命令重放）：慢臂持窗仅补投 TTL、不出任何帧（票 zcode-r153c-setrangeget
  /// 案一，杜绝重放自碰已提交值把新值冒充旧值回显 / NX 反判 / KEEPTTL 静默丢）
  ReplyEcho(TtlLeg),
}

impl TtlResume {
  /// 尾参线长单源（1 模式字节 + 8 字节 LE 过期刻度 + 8 字节 LE vns + 8 字节
  /// LE vdb 域锚；编码/逆解析共用，杜绝双字面量）
  const TAIL_LEN: usize = 1 + 8 + 16;

  /// 快照尾参编码（25 字节：模式字节 b'0'/b'1'/b'2'/b'3' + 8 字节 LE 过期
  /// 刻度 + 16 字节 LE 域锚 `(vns, vdb)`；沿 MSETNX 单字节模式标记与 DEL
  /// 计数 8 字节 LE 尾参同款先例；Full 域锚填零——全量重放不携域语义）
  pub fn tail_bytes(self) -> Vec<u8> {
    let (mode, TtlLeg { ticks, domain }) = match self {
      Self::Full => (
        b'0',
        TtlLeg {
          ticks: 0,
          domain: (0, 0),
        },
      ),
      Self::Pending(leg) => (b'1', leg),
      Self::KeepTtl(leg) => (b'2', leg),
      Self::ReplyEcho(leg) => (b'3', leg),
    };
    let mut w = TailWriter::with_len(Self::TAIL_LEN);
    w.push_mode(mode);
    w.push_le(ticks as u64);
    w.push_domain(domain);
    w.tail
  }

  /// 快照尾参逆解析（形态不符按 Full 兜底：全量重放语义最保守）
  pub fn from_tail(tail: Option<&[u8]>) -> Self {
    let Some(mut r) = TailReader::new(tail, Self::TAIL_LEN) else {
      return Self::Full;
    };
    let mode = r.next_mode();
    let leg = TtlLeg {
      ticks: r.next_le() as i64,
      domain: r.next_domain(),
    };
    match mode {
      b'1' => Self::Pending(leg),
      b'2' => Self::KeepTtl(leg),
      b'3' => Self::ReplyEcho(leg),
      _ => Self::Full,
    }
  }
}

/// ETag 写族慢路径承接标记（票 zcode-r139c-etag2 案一情形 B：快臂「值腿已同步
/// 提交」后 TTL / etag 余腿遭环形页翻转降级时置 Pending，exec 降级快照尾参承载，
/// 慢臂消费后持窗补投余腿出成功帧）。
///
/// 杜绝整命令重放自碰已提交值——[`TtlResume`] 只携 TTL 刻度，ETag
/// 族的成功帧还须携快臂已裁决的新 etag（Missing / WrongType 臂重放后读态翻成
/// Hit，条件重判即失配回 `[0, 新值]`、etag 侧写永缺），故本族尾参在
/// 同一 pending_slow 通道上多带一枚 etag（MSETNX 1 字节 / DEL 8 字节 /
/// HSCAN 4 字节 / VADD 2 字节同款「每族自带尾参」形态，不新建通道）
#[derive(Debug, Copy, PartialEq, Default, Clone)]
pub enum EtagResume {
  /// 值腿未提交（RI 门磁盘候选 / upsert 页翻转）或无降级：慢路径整命令全量重放
  #[default]
  Full,
  /// 值腿已同步提交、余腿待投：`ttl.ticks` 非零即 TTL 待投绝对过期刻度
  /// （0 = TTL 腿已闭环，仅 etag 待投）；`new_etag` 为快臂已裁决的 etag 侧写；
  /// `ttl.domain` 为值提交域锚（跨换号重放跳余腿补投，禁把 TTL / etag 落进
  /// 不相干新域）；`found` 为会话命中统计标志（true=found, false=notfound）
  Pending {
    ttl: TtlLeg,
    new_etag: i64,
    found: bool,
  },
}

impl EtagResume {
  /// 尾参线长单源（1 模式字节 + 8 字节 LE 新 etag + 8 字节 LE 过期刻度 +
  /// 16 字节 LE 域锚）
  const TAIL_LEN: usize = 1 + 8 + 8 + 16;

  /// 快照尾参编码（33 字节：模式字节 b'0'/b'1'/b'2' + 8 字节 LE 新 etag + 8 字节
  /// LE 过期刻度 + 16 字节 LE 域锚；[`TtlResume::tail_bytes`] 同款线形，
  /// 多带 etag 一枚与域锚一段；Full 域锚填零——全量重放不携域语义）
  pub fn tail_bytes(self) -> Vec<u8> {
    let (mode, new_etag, TtlLeg { ticks, domain }) = match self {
      Self::Full => (
        b'0',
        0i64,
        TtlLeg {
          ticks: 0,
          domain: (0, 0),
        },
      ),
      Self::Pending {
        ttl,
        new_etag,
        found: true,
      } => (b'1', new_etag, ttl),
      Self::Pending {
        ttl,
        new_etag,
        found: false,
      } => (b'2', new_etag, ttl),
    };
    let mut w = TailWriter::with_len(Self::TAIL_LEN);
    w.push_mode(mode);
    w.push_le(new_etag as u64);
    w.push_le(ticks as u64);
    w.push_domain(domain);
    w.tail
  }

  /// 快照尾参逆解析（形态不符按 Full 兜底：全量重放语义最保守；模式字节
  /// 仅 b'1'/b'2' 合法，其余含长度恰符的伪形态一律 Full）
  pub fn from_tail(tail: Option<&[u8]>) -> Self {
    let Some(mut r) = TailReader::new(tail, Self::TAIL_LEN) else {
      return Self::Full;
    };
    let mode = r.next_mode();
    if mode != b'1' && mode != b'2' {
      return Self::Full;
    }
    let new_etag = r.next_le() as i64;
    let ttl = TtlLeg {
      ticks: r.next_le() as i64,
      domain: r.next_domain(),
    };
    Self::Pending {
      ttl,
      new_etag,
      found: mode == b'1',
    }
  }
}
