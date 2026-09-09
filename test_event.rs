use event_listener::Event;
fn test() {
    let ev = Event::new();
    let l = ev.listen();
    let _ = l.wait_timeout(std::time::Duration::from_secs(1));
}
