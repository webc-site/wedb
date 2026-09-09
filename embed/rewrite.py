import os
import re

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    content = content.replace("std::sync::Mutex", "parking_lot::Mutex")
    content = content.replace("sync::Mutex", "parking_lot::Mutex")
    
    content = re.sub(r"use byteorder::[^;]+;\n?", "", content)
    if "bitcode" not in content:
        content = "use bitcode::{Encode, Decode};\n" + content
    
    if "to_string().into_bytes()" in content:
        content = re.sub(r"([a-zA-Z0-9_\.\(\)]+)\.to_string\(\)\.into_bytes\(\)", 
                         r"itoa::Buffer::new().format(\1).as_bytes().to_vec()", content)
        
        content = content.replace("itoa::Buffer::new().format(current_val).as_bytes().to_vec()", 
                                  "zmij::Buffer::new().format(current_val).as_bytes().to_vec()")
        content = content.replace("itoa::Buffer::new().format(new_score).as_bytes().to_vec()", 
                                  "zmij::Buffer::new().format(new_score).as_bytes().to_vec()")

    with open(filepath, 'w') as f:
        f.write(content)

base = "/tmp/fork/review-embed-storage/embed/wobject/src"
for d in ["hash/hash_object.rs", "set/set_object.rs", "list/list_object.rs", "sorted_set/sorted_set_object.rs"]:
    process_file(os.path.join(base, d))
