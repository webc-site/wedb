with open('/tmp/fork/client-host/embed/wutil/src/num.rs', 'r') as f:
    text = f.read()

text = "use core::str::from_utf8;\n" + text
text = text.replace('core::str::from_utf8', 'from_utf8')
# but wait, the import itself would be 'use core::str::from_utf8;', so text.replace would replace the import too!
text = text.replace('use from_utf8;\n', 'use core::str::from_utf8;\n')

with open('/tmp/fork/client-host/embed/wutil/src/num.rs', 'w') as f:
    f.write(text)
