pub mod alloc;
pub mod engines;
pub mod error;
pub mod harness;
pub mod i18n;
pub mod report;
pub mod sys_info;
pub mod traits;
pub mod types;

pub use engines::EngineBenchResult;
pub use error::{Error, Result};
pub use harness::run_benchmark;
pub use i18n::I18nTexts;
pub use report::{generate_markdown_report, print_console_table, write_report};
pub use sys_info::MachineInfo;
pub use types::{
  BenchmarkConfig, JsonBenchmarkData, JsonEngineResult, JsonMetric, MemoryBudget, ResultType,
};
