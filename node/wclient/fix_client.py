with open('/tmp/fork/client-host/node/wclient/src/client.rs', 'r') as f:
    code = f.read()
code = code.replace("    async fn network_loop", "    /// libs/client/GarnetClientProcessReplies.cs:ProcessReplies\n    async fn network_loop")
with open('/tmp/fork/client-host/node/wclient/src/client.rs', 'w') as f:
    f.write(code)
