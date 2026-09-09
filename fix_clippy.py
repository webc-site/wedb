with open('/tmp/fork/client-host/node/wclient/src/parser.rs', 'r') as f:
    code = f.read()

# Replace manual_find with find
import re
code = code.replace("for i in 0..data.len().saturating_sub(1) {\n      if data[i] == b'\\r' && data[i + 1] == b'\\n' {\n        return Some(i);\n      }\n    }\n    None", "(0..data.len().saturating_sub(1)).find(|&i| data[i] == b'\\r' && data[i + 1] == b'\\n')")
with open('/tmp/fork/client-host/node/wclient/src/parser.rs', 'w') as f:
    f.write(code)

with open('/tmp/fork/client-host/node/wclient/src/error.rs', 'r') as f:
    code = f.read()

code = code.replace("std::result::Result", "Result")
code = code.replace("std::io::Error", "std::io::Error")
with open('/tmp/fork/client-host/node/wclient/src/error.rs', 'w') as f:
    f.write(code)

