import re
with open('/tmp/fork/client-host/node/wclient/src/session.rs', 'r') as f:
    code = f.read()

new_code = code.replace("""                let mut data_slice = read_buf.as_slice();
                while !data_slice.is_empty() && !tcs_queue.is_empty() {""",
"""                let mut data_slice = read_buf.as_slice();
                let mut total_consumed = 0;
                while !data_slice.is_empty() && !tcs_queue.is_empty() {""")

new_code = new_code.replace("""                    if consumed_bytes > 0 {
                        let rem = read_buf.len() - old_len + consumed_bytes;
                        read_buf.drain(..rem);
                    } else {
                        break;
                    }""",
"""                    if consumed_bytes > 0 {
                        total_consumed += consumed_bytes;
                    } else {
                        break;
                    }""")

new_code = new_code.replace("""                }
            }
        }
        Ok(())""",
"""                }
                read_buf.drain(..total_consumed);
            }
        }
        Ok(())""")

with open('/tmp/fork/client-host/node/wclient/src/session.rs', 'w') as f:
    f.write(new_code)
