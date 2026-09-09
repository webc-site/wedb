import re
with open('/tmp/fork/client-host/node/whost/Cargo.toml', 'r') as f:
    text = f.read()

text = re.sub(r'sonic-rs = ".*"\n', '', text)
text = re.sub(r'wkv = \{ workspace = true \}\n', '', text)

with open('/tmp/fork/client-host/node/whost/Cargo.toml', 'w') as f:
    f.write(text)
