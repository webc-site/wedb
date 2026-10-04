use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::{align::DEFAULT_SECTOR_SIZE, pool::AlignedBuf};
use wdev::{Device, DeviceParams, SegmentedDevice};

#[test]
fn test_create_with_missing_multi_level_parent_dir() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    // Create a multi-level missing parent path
    let missing_parent = dir.path().join("a").join("b").join("c");
    let dev_path = missing_parent.join("test.log");

    let params = DeviceParams {
      capacity: None,
      preallocate: false,
      read_only: false,
      delete_on_close: true,
    };

    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::with_params(&dev_path, seg_size, DEFAULT_SECTOR_SIZE, params)?;

    let buf = AlignedBuf::from_slice(&[0x42u8; 4096], 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);

    device.sync().await?;

    let check = AlignedBuf::new(4096, 4096)?;
    let (res, check) = device.read_aligned(0, check).await;
    assert_eq!(res?, 4096);
    assert_eq!(check.as_slice(), &[0x42u8; 4096]);

    info!("missing_parent_dir_fsync 校验通过");
    aok::Result::<()>::Ok(())
  })?;
  OK
}
