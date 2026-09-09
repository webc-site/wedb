import re

with open('wobject/src/list/list_object.rs', 'r') as f:
    c = f.read()
c = c.replace("std::io::{Read, Write}", "")
c = c.replace("std::parking_lot::RwLock", "parking_lot::RwLock")
c = re.sub(r"pub fn deserialize.*?(?=\n  ///|\n  pub fn operate)", """pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<Vec<u8>> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let list = std::collections::VecDeque::from(vec);
    Ok(Self {
      list: parking_lot::RwLock::new(list),
    })
  }

  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let list = self.list.read();
    let vec: Vec<&Vec<u8>> = list.iter().collect();
    Ok(bitcode::encode(&vec))
  }""", c, flags=re.DOTALL)
with open('wobject/src/list/list_object.rs', 'w') as f:
    f.write(c)

with open('wobject/src/sorted_set/sorted_set_object.rs', 'r') as f:
    c = f.read()
c = c.replace("std::io::{Read, Write},", "")
c = c.replace("std::parking_lot::RwLock", "")
c = c.replace("use std::\n", "use std::")
c = c.replace(".unwrap()", "")
c = re.sub(r"pub fn deserialize.*?(?=\n  ///|\n  pub fn operate)", """pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<(Vec<u8>, f64)> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let dict = papaya::HashMap::with_hasher(GxBuildHasher::default());
    let pin = dict.pin();
    let mut tree = BTreeSet::new();
    for (k, v) in vec {
      let fscore = OrderedFloat(v);
      pin.insert(k.clone(), fscore);
      tree.insert(SortedSetEntry {
        score: fscore,
        member: k,
      });
    }
    drop(pin);
    Ok(Self {
      dict,
      tree: RwLock::new(tree),
    })
  }

  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let pin = self.dict.pin();
    let vec: Vec<(&Vec<u8>, f64)> = pin.iter().map(|(k, v)| (k, v.into_inner())).collect();
    Ok(bitcode::encode(&vec))
  }""", c, flags=re.DOTALL)
with open('wobject/src/sorted_set/sorted_set_object.rs', 'w') as f:
    f.write(c)

with open('wobject/src/hash/hash_object.rs', 'r') as f:
    c = f.read()
c = c.replace("io::{Read, Write},", "")
with open('wobject/src/hash/hash_object.rs', 'w') as f:
    f.write(c)

with open('wobject/src/set/set_object.rs', 'r') as f:
    c = f.read()
c = c.replace("use std::io::{Read, Write};", "")
with open('wobject/src/set/set_object.rs', 'w') as f:
    f.write(c)
