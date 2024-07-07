## Set command
The `set_command.rs` file is responsible for implementing the functionality related to the Redis `SET`. It defines a `SetCommandActorHandle` struct and its associated methods to interact with a `SetCommandActor` through asynchronous messaging. The key features and functionalities include:

- **Struct Definition**: `SetCommandActorHandle` contains a `sender` field of type `mpsc::Sender<SetActorMessage>`, which is used to communicate with the `SetCommandActor`.
- **Constructor**: The `new` method initializes a `SetCommandActorHandle` by creating a channel and spawning a Tokio task that runs the actor asynchronously.
- **GET Command**: Implements the Redis `GET` command through the `get_value` method, allowing retrieval of values associated with keys. It sends a `GetValue` message to the actor and awaits a response.
- **KEYS Command**: Implements the Redis `KEYS` command via the `get_keys` method, enabling pattern-based key retrieval. It sends a `GetKeys` message to the actor and awaits a list of matching keys.
- **SET Command**: Provides functionality to set key-value pairs using the `set_value` method. It constructs a `SetValue` message and sends it to the actor, also handling expiration parameters through a separate channel (`expire_tx`).
- **DELETE Command**: Supports immediate deletion of keys with the `delete_value` method, sending a `DeleteValue` message to the actor for removal.

This module leverages Tokio's asynchronous runtime and messaging passing for non-blocking communication between components, adhering to the actor model for concurrency.