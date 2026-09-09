sed -i '' 's/let rem = read_buf.len() - old_len + consumed_bytes;/let rem = read_buf.len() - old_len + consumed_bytes;/' /tmp/fork/client-host/node/wclient/src/session.rs
