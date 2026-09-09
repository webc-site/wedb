with open('/tmp/fork/client-host/node/wclient/src/error.rs', 'r') as f:
    text = f.read()

text = text.replace('pub type Result<T> = Result<T, Error>;', 'pub type Result<T> = core::result::Result<T, Error>;')
text = text.replace('std::io::Error', 'std::io::Error')

with open('/tmp/fork/client-host/node/wclient/src/error.rs', 'w') as f:
    f.write(text)
