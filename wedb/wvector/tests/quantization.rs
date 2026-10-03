//! 量化器基础测试（自 src/quantization.rs 外迁）

use diskann_utils::views::Matrix;
use diskann_vector::distance::Metric;
use wvector::{
  error::QuantizerError,
  quantization::{MinMax8Bit, Spherical1Bit, WedbQuantizer},
};

/// 两形态共用断言：压缩记录非全零，且两两距离与查询距离通道均可求值非零。
fn assert_quant<Q: WedbQuantizer>(quantizer: &Q, test_v: &[f32]) {
  let mut test_q = vec![0u8; quantizer.bytes()];
  quantizer.compress(test_v, &mut test_q).unwrap();
  assert!(!test_q.iter().all(|&b| b == 0));

  let mut quant_a = vec![0u8; quantizer.bytes()];
  quantizer.compress(&[0.0f32, 0.0], &mut quant_a).unwrap();
  let dist_comp = quantizer.distance_computer().unwrap();
  assert_ne!(dist_comp.evaluate_similarity(&quant_a, &test_q), 0.0);
  let query_comp = quantizer.query_computer(test_v).unwrap();
  assert_ne!(query_comp.evaluate_similarity(&quant_a), 0.0);
}

#[test]
fn basic_spherical_1bit() {
  let quantizer = Spherical1Bit::new(2, 2);

  assert_eq!(quantizer.required_vectors(), 1000);
  assert_eq!(quantizer.bytes(), 1 + 6);
  assert!(!quantizer.is_trained());

  let test_v = [0.5f32, 0.5];
  let mut test_q = vec![0u8; quantizer.bytes()];

  assert!(matches!(
    quantizer.compress(&test_v, &mut test_q),
    Err(QuantizerError::NoQuantizer)
  ));
  assert!(matches!(
    quantizer.distance_computer(),
    Err(QuantizerError::NoQuantizer)
  ));
  assert!(matches!(
    quantizer.query_computer(&test_v),
    Err(QuantizerError::NoQuantizer)
  ));

  let mut test_data = Matrix::new(0.0f32, 1000, 2);
  for (i, row) in test_data.row_iter_mut().enumerate() {
    row.copy_from_slice(&[(i + 1) as f32, (i + 1) as f32]);
  }
  quantizer.train(Metric::L2, test_data.as_view()).unwrap();

  assert!(quantizer.is_trained());

  assert_quant(&quantizer, &test_v);
}

#[test]
fn basic_minmax_8bit() {
  let quantizer = MinMax8Bit::new(2, 2, Metric::L2).unwrap();

  assert_eq!(quantizer.required_vectors(), 0);
  assert_eq!(quantizer.bytes(), 22);
  // MinMax8Bit 天然已训练
  assert!(quantizer.is_trained());

  let test_v = [0.5f32, 0.5];

  let mut test_data = Matrix::new(0.0f32, 1, 2);
  test_data.row_mut(0).copy_from_slice(&[1.0f32, 1.0]);

  // 训练为空操作，但成功
  quantizer.train(Metric::L2, test_data.as_view()).unwrap();

  assert_quant(&quantizer, &test_v);
}
