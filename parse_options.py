import re

with open('/Users/z/git/db/wedb/garnet/libs/host/Configuration/Options.cs', 'r') as f:
    text = f.read()

props = re.findall(r'public ([\w\?<>]+) (\w+) \{ get; set; \}', text)

rust_code = "use serde::{Serialize, Deserialize};\n\n"
rust_code += "/// libs/host/Configuration/Options.cs:Options\n"
rust_code += "#[derive(Debug, Clone, Serialize, Deserialize, Default)]\n"
rust_code += "pub struct Options {\n"

def cs_to_rs_type(t):
    if t == 'int' or t == 'int?': return 'Option<i32>'
    if t == 'long' or t == 'long?': return 'Option<i64>'
    if t == 'double' or t == 'double?': return 'Option<f64>'
    if t == 'bool' or t == 'bool?': return 'Option<bool>'
    if t == 'string': return 'Option<String>'
    if t.startswith('IEnumerable<'): return 'Option<Vec<String>>' # simplifying
    if t.startswith('IList<'): return 'Option<Vec<String>>'
    if t == 'ILogger': return 'Option<String>'
    return f"Option<{t.replace('?', '')}>"

for t, name in props:
    rs_name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    rs_type = cs_to_rs_type(t)
    rust_code += f"    pub {rs_name}: {rs_type},\n"

rust_code += "}\n"

with open('/tmp/fork/client-host/node/whost/src/config/options.rs', 'w') as f:
    f.write(rust_code)
