# Redis in Rust
This started as a toy project for ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis) it is now an attempt to do a complete re-write of Redis in Rust.

# Commands
The following Redis commands have been implemented:
- [x] SET [EX|PX]
- [x] GET
- [x] PING
- [ ] COMMAND DOCS (always returns +OK)
- [x] ECHO
- [x] DEL
- [x] MGET
- [x] STRLEN
- [x] APPEND
- [x] CONFIG GET

In addition, you can pass command-line `--dir` and `--dbfilename` parameters to restore state from disk.

# Design Overview

## Supported commands
All the supported commands are defined as enums in [protocol.rs](src/protocol.rs).

## Main loop

The `main.rs` tokio loop handles inbound connections:

```rust
loop {
    // Asynchronously wait for an inbound TcpStream.
    let (stream, _) = listener.accept().await?;

    // Must clone the handler because tokio::spawn move will grab everything.
    let set_command_handler_clone = set_command_actor_handle.clone();

    // Spawn our handler to be run asynchronously.
    // A new task is spawned for each inbound socket.  
    // The socket is moved to the new task and processed there.
    tokio::spawn(async move {
        process(stream, set_command_handler_clone)
            .await
            .expect("Failed to spawn process thread");
    });
}
```
### Expiry loop
As part of the main initialization, we kick off a separate thread with an expiry channel. It listens for incoming requests to expire keys and acts accordingly.

NOTE: At the moment, this is a per redis expiry since this implementation supports a single database only.

## Per-process loop
To ensure the server can handle multiple connections at the same time, every connection spawns a new thread and gets moved there immediately:

```rust
async fn process(stream: TcpStream, set_command_actor_handle: SetCommandActorHandle) -> Result<()> {}
```

## Actor model
To avoid sharing state and dealing with mutexes, this code uses an actor model.
