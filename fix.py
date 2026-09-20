import re
import glob
import os

base_dir = '/tmp/fork/resp-commands-inline/wedb/wnode/src/resp'
for root, dirs, files in os.walk(base_dir):
    for f in files:
        if f.endswith('.rs'):
            path = os.path.join(root, f)
            with open(path, 'r') as file:
                content = file.read()
            
            # fix empty imports like `use ..., , };` or `use crate::{ , };` or `use super::{ , };`
            content = re.sub(r',\s*,', ',', content)
            content = re.sub(r'\{\s*,', '{', content)
            content = re.sub(r',\s*\}', '}', content)
            
            with open(path, 'w') as file:
                file.write(content)

