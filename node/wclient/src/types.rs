use crossfire::oneshot::TxOneshot;

use crate::Result;

pub enum CommandItem {
  Command {
    cmd: Vec<String>,
    resp_tx: TxOneshot<Result<String>>,
  },
  CommandForArray {
    cmd: Vec<String>,
    resp_tx: TxOneshot<Result<Vec<String>>>,
  },
}
