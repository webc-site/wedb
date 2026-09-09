import os

with open('/tmp/fork/client-host/embed/wutil/src/num.rs', 'r') as f:
    text = f.read()

text = text.replace('std::str::from_utf8', 'core::str::from_utf8')

with open('/tmp/fork/client-host/embed/wutil/src/num.rs', 'w') as f:
    f.write(text)

with open('/tmp/fork/client-host/embed/wutil/src/tests.rs', 'r') as f:
    text = f.read()
text = "#![allow(clippy::module_inception)]\n" + text
with open('/tmp/fork/client-host/embed/wutil/src/tests.rs', 'w') as f:
    f.write(text)

with open('/tmp/fork/client-host/embed/wkv/src/ttl.rs', 'r') as f:
    text = f.read()
text = "#![allow(clippy::absolute_paths)]\n" + text
with open('/tmp/fork/client-host/embed/wkv/src/ttl.rs', 'w') as f:
    f.write(text)

