//! 服务端证书重载、代际管理与并发赛跑集成测试
//! 对标 test/standalone/Garnet.test/RespTlsTests.cs 与 libs/server/TLS/ServerCertificateSelector.cs

use std::{fs::write, io, path::PathBuf, thread};

use compio::runtime::Runtime;
use compio_tls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use wtls::ServerTlsConfig;

/// 内存自签装配（rcgen 产 DER，零外部文件依赖）；from_der 为内存形态，
/// 刷新周期恒钳 0
fn der_config(client_cert_required: bool) -> io::Result<ServerTlsConfig> {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("自签证书生成");
  ServerTlsConfig::from_der(
    vec![CertificateDer::from(ck.cert.der().to_vec())],
    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
      ck.signing_key.serialized_der().to_vec(),
    )),
    client_cert_required,
    None,
    0,
  )
}

/// A/B 双证书 PEM 夹具：临时目录内落 cert_{a,b}.pem / key_{a,b}.pem，
/// 附带 A/B 证书 DER 快照（换证竞态三测共用，`_dir` 守卫持有目录生命周期）
struct AbCertFixture {
  _dir: tempfile::TempDir,
  cert_a_path: PathBuf,
  key_a_path: PathBuf,
  cert_b_path: PathBuf,
  key_b_path: PathBuf,
  a_cert_der: Vec<u8>,
  b_cert_der: Vec<u8>,
}

fn ab_cert_fixture() -> AbCertFixture {
  let dir = tempfile::tempdir().expect("临时目录");
  let cert_a_path = dir.path().join("cert_a.pem");
  let key_a_path = dir.path().join("key_a.pem");
  let cert_b_path = dir.path().join("cert_b.pem");
  let key_b_path = dir.path().join("key_b.pem");

  let cert_a = rcgen::generate_simple_self_signed(vec!["server_a.com".into()]).expect("自签 A");
  let cert_b = rcgen::generate_simple_self_signed(vec!["server_b.com".into()]).expect("自签 B");

  write(&cert_a_path, cert_a.cert.pem()).expect("写 A 证书");
  write(&key_a_path, cert_a.signing_key.serialize_pem()).expect("写 A 私钥");
  write(&cert_b_path, cert_b.cert.pem()).expect("写 B 证书");
  write(&key_b_path, cert_b.signing_key.serialize_pem()).expect("写 B 私钥");

  AbCertFixture {
    _dir: dir,
    a_cert_der: cert_a.cert.der().to_vec(),
    b_cert_der: cert_b.cert.der().to_vec(),
    cert_a_path,
    key_a_path,
    cert_b_path,
    key_b_path,
  }
}

/// 刷新代际与在挂票据：非零周期在运行时外只记意图不拉起（false）；
/// 室内拉起即锁代际，换代自增使旧代失配自退；宿主运行时析构清理在挂
/// 任务时票据复位哨兵；周期为 0 恒无循环。非零周期两臂走文件来源形态
///（刷新循环按路径重读，from_der 内存形态周期恒钳 0 无从测起）
#[test]
fn refresh_loop_epoch_and_ticket_lifecycle() {
  // 文件夹具：自签 PEM 落临时目录，持守卫至本测结束（循环重读依赖路径在位）
  let dir = tempfile::tempdir().expect("临时目录");
  let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("自签证书生成");
  let cert_path = dir.path().join("cert.pem");
  let key_path = dir.path().join("key.pem");
  write(&cert_path, cert.cert.pem()).expect("写证书");
  write(&key_path, cert.signing_key.serialize_pem()).expect("写私钥");
  let pem_config =
    |freq: u64| ServerTlsConfig::from_pem_files(&cert_path, &key_path, false, None, freq);

  // 室外：refresh_freq_secs > 0 意图记录后拉起失败（false），未标记在挂
  let outdoor = pem_config(1).expect("装配");
  assert!(!outdoor.ensure_refresh_loop(), "运行时外拉起必须报 false");
  assert_eq!(outdoor.refresh_epoch(), 0);
  assert_eq!(
    outdoor.running_epoch(),
    ServerTlsConfig::NOT_RUNNING,
    "未拉起不得标记在挂"
  );

  // 室内：拉起成功且锁代际；换代后旧代票据不再回收新代位
  let probe = {
    let rt = Runtime::new().expect("运行时");
    let probe = rt.block_on(async {
      let config = pem_config(1).expect("装配");
      assert!(config.ensure_refresh_loop(), "室内拉起必须成功");
      assert_eq!(config.refresh_epoch(), 0);
      assert_eq!(config.running_epoch(), 0, "拉起即锁代际");

      // 换代：refresh_epoch 自增使 0 代循环失配自退，新代接管 running_epoch
      config.bump_refresh_epoch();
      assert!(config.ensure_refresh_loop(), "换代后再拉起");
      assert_eq!(config.refresh_epoch(), 1);
      assert_eq!(config.running_epoch(), 1);

      config
    });
    // 克隆共享同一 Inner，宿主运行时随本块析构后经探针核销票据复位
    probe.clone()
  };

  // 周期 0（from_der 钳 0 后恒此形态）：无条件 true 且无状态迁移
  let inert = der_config(false).expect("装配");
  assert!(inert.ensure_refresh_loop());
  assert_eq!(
    inert.running_epoch(),
    ServerTlsConfig::NOT_RUNNING,
    "周期 0 不得挂循环"
  );

  // 宿主运行时已析构（块退出）：在挂任务连票据一并清理，代际复位哨兵，
  // 下次服务启动可重挂
  assert_eq!(
    probe.running_epoch(),
    ServerTlsConfig::NOT_RUNNING,
    "运行时析构必须复位票据"
  );
}

/// 换证在途重读回灌竞态防御：
/// 当 refresh_epoch 发生推进（换证换代）时，在途旧代 reload 丢弃本次重读结果不 store，
/// 活跃证书保持新换证书，不被旧重读覆盖回灌
#[test]
fn reload_race_epoch_mismatch_drops_stale_cert() {
  let fx = ab_cert_fixture();

  let config = ServerTlsConfig::from_pem_files(&fx.cert_a_path, &fx.key_a_path, false, None, 10)
    .expect("装配 A");

  assert_eq!(
    config.cert_source().load().cert[0].as_ref(),
    fx.a_cert_der.as_slice()
  );

  // 切换至证书 B（refresh_epoch 推进为 1）
  config
    .update_cert_file(Some(&fx.cert_b_path), Some(&fx.key_b_path))
    .expect("更新至 B 证书");

  assert_eq!(
    config.cert_source().load().cert[0].as_ref(),
    fx.b_cert_der.as_slice()
  );

  // 旧代 my_epoch = 0 尝试 reload
  config.reload(0).expect("旧代 reload 应静默返回 Ok");

  // 断言活跃证书仍然是 B，未被旧代回灌
  assert_eq!(
    config.cert_source().load().cert[0].as_ref(),
    fx.b_cert_der.as_slice(),
    "活跃证书必须保持为新证书 B，决不可被旧代重载回灌"
  );

  // 新代 my_epoch = 1 执行 reload 正常换装
  config.reload(1).expect("新代 reload 应成功");
  assert_eq!(
    config.cert_source().load().cert[0].as_ref(),
    fx.b_cert_der.as_slice()
  );
}

/// 验证在途重读回灌竞态：
/// 若 reload 在换证前已读取旧路径证书 A，在 store 前发生 update_cert_file 切换至证书 B（推进 refresh_epoch），
/// reload 必须核对代际失配并丢弃旧证书 A，防止旧证书 A 覆盖新证书 B
#[test]
fn reload_in_flight_stale_cert_dropped_on_epoch_mismatch() {
  let fx = ab_cert_fixture();

  let config = ServerTlsConfig::from_pem_files(&fx.cert_a_path, &fx.key_a_path, false, None, 10)
    .expect("装配 A");

  // 换证至 B
  config
    .update_cert_file(Some(&fx.cert_b_path), Some(&fx.key_b_path))
    .expect("更新至 B 证书");

  // 假设在途重读拿到的是 cert_a_path（模拟在换证前克隆出的旧路径）
  config.set_paths_for_test(fx.cert_a_path.clone(), fx.key_a_path.clone());

  // 在途旧循环（my_epoch = 0）调用 reload：读取 cert_a，但因 my_epoch(0) != state.refresh_epoch(1) 丢弃
  config.reload(0).expect("旧代 reload 应静默返回 Ok");

  // 活跃证书必须仍然是 B，决不被 A 覆盖
  assert_eq!(
    config.cert_source().load().cert[0].as_ref(),
    fx.b_cert_der.as_slice(),
    "在途重载旧证书必须被丢弃，活跃证书必须保持为 B"
  );
}

/// 第三态交错用例（并发迭代式，不对生产 API 做分段暴露）：旧代 reload 的
/// 核对-换装临界区与 update_cert_file 的换装-换代临界区收敛于同一把 state
/// 锁后，两种落位序终态均为新证——reload 先落位则被 update 新证覆盖，
/// update 先落位则 reload 代际核对失配丢弃。逐轮 spawn 匹配代 reload 与
/// 主线程 update 并发赛跑，join 后断言活跃证书恒为当轮新证
#[test]
fn concurrent_reload_update_race_always_lands_new_cert() {
  let fx = ab_cert_fixture();

  let config = ServerTlsConfig::from_pem_files(&fx.cert_a_path, &fx.key_a_path, false, None, 10)
    .expect("装配 A");

  // A/B 交替换装逐轮赛跑：每轮 spawn 的 reload 代际恰为轮次（update 每轮
  // 自增一次）；主线程 yield 让渡调度以提高 reload 先落位序占比，两种序
  // 同受 join 后终态断言约束
  for round in 0..64u64 {
    let to_b = round % 2 == 0;
    let (target_path, target_key, target_der) = if to_b {
      (&fx.cert_b_path, &fx.key_b_path, &fx.b_cert_der)
    } else {
      (&fx.cert_a_path, &fx.key_a_path, &fx.a_cert_der)
    };

    let racer = {
      let config = config.clone();
      thread::spawn(move || config.reload(round))
    };
    thread::yield_now();
    config
      .update_cert_file(Some(target_path), Some(target_key))
      .expect("换装");
    racer.join().expect("赛跑线程").expect("reload");

    assert_eq!(
      config.cert_source().load().cert[0].as_ref(),
      target_der.as_slice(),
      "第 {round} 轮 join 后活跃证书必须为当轮新证，回灌即竞态复活"
    );
  }
  assert_eq!(config.refresh_epoch(), 64, "每轮换装必须恰好自增一次代际");
}
