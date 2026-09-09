import os

files = {
    "hash/hash_object.rs": (
        """  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<(Vec<u8>, Vec<u8>)> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let hash = HashMap::with_hasher(GxBuildHasher::default());
    let pin = hash.pin();
    for (k, v) in vec {
      pin.insert(k, v);
    }
    drop(pin);
    Ok(Self {
      hash,
      expiration_times: HashMap::with_hasher(GxBuildHasher::default()),
      expiration_queue: Mutex::new(BinaryHeap::new()),
    })
  }

  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let pin = self.hash.pin();
    let vec: Vec<(&Vec<u8>, &Vec<u8>)> = pin.iter().collect();
    Ok(bitcode::encode(&vec))
  }""",
        r"""  pub fn deserialize<R: Read>\(reader: &mut R\) -> std::io::Result<Self> \{.*?Ok\(Self \{\s*hash,\s*expiration_times:.*?\s*expiration_queue:.*?\s*\}\)\s*\}""",
        r"""  pub fn serialize<W: Write>\(&self, writer: &mut W\) -> std::io::Result<\(\)> \{.*?Ok\(\(\)\)\s*\}"""
    ),
    "set/set_object.rs": (
        """  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<Vec<u8>> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let set = HashSet::with_hasher(GxBuildHasher::default());
    let pin = set.pin();
    for item in vec {
      pin.insert(item);
    }
    drop(pin);
    Ok(Self { set })
  }

  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let pin = self.set.pin();
    let vec: Vec<&Vec<u8>> = pin.iter().collect();
    Ok(bitcode::encode(&vec))
  }""",
        r"""  pub fn deserialize<R: Read>\(reader: &mut R\) -> std::io::Result<Self> \{.*?Ok\(Self \{ set \}\)\s*\}""",
        r"""  pub fn serialize<W: Write>\(&self, writer: &mut W\) -> std::io::Result<\(\)> \{.*?Ok\(\(\)\)\s*\}"""
    ),
    "list/list_object.rs": (
        """  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<Vec<u8>> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let list = std::collections::LinkedList::from_iter(vec);
    Ok(Self {
      list: parking_lot::RwLock::new(list),
    })
  }

  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let list = self.list.read();
    let vec: Vec<&Vec<u8>> = list.iter().collect();
    Ok(bitcode::encode(&vec))
  }""",
        r"""  pub fn deserialize<R: Read>\(reader: &mut R\) -> std::io::Result<Self> \{.*?Ok\(Self \{\s*list:.*?\s*\}\)\s*\}""",
        r"""  pub fn serialize<W: Write>\(&self, writer: &mut W\) -> std::io::Result<\(\)> \{.*?Ok\(\(\)\)\s*\}"""
    ),
    "sorted_set/sorted_set_object.rs": (
        """  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<(Vec<u8>, f64)> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let dict = papaya::HashMap::with_hasher(GxBuildHasher::default());
    let pin = dict.pin();
    for (k, v) in &vec {
      pin.insert(k.clone(), *v);
    }
    drop(pin);
    Ok(Self {
      dict,
      scores: parking_lot::RwLock::new(BTreeSet::from_iter(vec.into_iter().map(|(k, v)| SortedSetEntry {
        score: OrderedFloat(v),
        key: k,
      }))),
    })
  }

  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let pin = self.dict.pin();
    let vec: Vec<(&Vec<u8>, &f64)> = pin.iter().collect();
    Ok(bitcode::encode(&vec))
  }""",
        r"""  pub fn deserialize<R: Read>\(reader: &mut R\) -> std::io::Result<Self> \{.*?Ok\(Self \{\s*dict,\s*scores:.*?\s*\}\)\s*\}""",
        r"""  pub fn serialize<W: Write>\(&self, writer: &mut W\) -> std::io::Result<\(\)> \{.*?Ok\(\(\)\)\s*\}"""
    )
}

import re

base = "/tmp/fork/review-embed-storage/embed/wobject/src"
for f, (repl, p_deser, p_ser) in files.items():
    filepath = os.path.join(base, f)
    with open(filepath, 'r') as fp:
        content = fp.read()
    
    # replace deserialize
    content = re.sub(p_deser, "  // MARKER", content, flags=re.DOTALL)
    # replace serialize
    content = re.sub(p_ser, "", content, flags=re.DOTALL)
    # replace MARKER with repl
    content = content.replace("  // MARKER", repl)
    
    # fix io imports
    content = re.sub(r"use std::io::\{Read, Write\};\n", "", content)
    
    # handle parking_lot::RwLock
    content = content.replace("std::sync::RwLock", "parking_lot::RwLock")
    content = content.replace("sync::RwLock", "parking_lot::RwLock")
    
    with open(filepath, 'w') as fp:
        fp.write(content)

