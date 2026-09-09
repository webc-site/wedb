import re

with open('wobject/Cargo.toml', 'r') as f:
    c = f.read()
c = c.replace('byteorder = "1.5.0"\n', '')
with open('wobject/Cargo.toml', 'w') as f: f.write(c)

with open('wobject/src/list/list_object.rs', 'r') as f:
    c = f.read()
c = c.replace('let list = self.list.read();', 'let list = self.list.lock();')
c = c.replace('std::io::{Read, Write}', '')
with open('wobject/src/list/list_object.rs', 'w') as f: f.write(c)

with open('wobject/src/sorted_set/sorted_set_object.rs', 'r') as f:
    c = f.read()
c = c.replace('.unwrap()', '')
c = c.replace('std::io::{Read, Write}', '')
c = c.replace('use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};', '')
c = re.sub(r'let count = reader\.read_i32.*', '', c)
c = re.sub(r'let score = reader\.read_f64.*', '', c)
c = re.sub(r'let member_len = reader\.read_i32.*', '', c)
c = c.replace('Ok(bitcode::encode(&vec))', 'Ok(bitcode::encode(&vec))')
c = c.replace('Ok(bitcode::encode(vec))', 'Ok(bitcode::encode(&vec))')

with open('wobject/src/sorted_set/sorted_set_object.rs', 'w') as f: f.write(c)

