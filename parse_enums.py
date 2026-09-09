import re

text = open('/Users/z/git/db/wedb/garnet/libs/host/Configuration/Options.cs').read()

enums = set()
props = re.findall(r'public ([\w\?]+) \w+ \{ get; set; \}', text)
for t in props:
    t = t.replace('?', '')
    if t not in ['int', 'long', 'double', 'bool', 'string', 'ILogger'] and not t.startswith('IEnumerable') and not t.startswith('IList'):
        if t == "NativeStorageDevice.IoBackend": t = "IoBackend"
        enums.add(t)

code = ""
for e in enums:
    code += f"#[derive(Debug, Clone, Serialize, Deserialize, Default)]\npub enum {e} {{ #[default] Default }}\n"

with open('/tmp/fork/client-host/node/whost/src/config/options.rs', 'r') as f:
    orig = f.read()

orig = orig.replace('NativeStorageDevice.IoBackend', 'IoBackend')
orig = orig.replace('Option<ILogger>', 'Option<String>')

with open('/tmp/fork/client-host/node/whost/src/config/options.rs', 'w') as f:
    f.write(code + "\n" + orig)
