# Introduction
Re-implementing Redis in Rust adopts an actor-based architecture for managing concurrency and state, leveraging the Tokio asynchronous runtime. 

This architecture choice aligns with the project's goal of avoiding shared state and mutexes, thereby simplifying synchronization and improving performance. Here's a brief overview of the actor-based implementation within the project:

# Actor Model Overview

- **Actor Definition**: Actors are independent units of computation that communicate exclusively through message passing. Each actor runs in its own task, allowing for concurrent execution without the need for explicit locks or condition variables. This model fits well with Redis's command processing nature, where commands can often be executed independently and in parallel.

## Key Components

- **Actors Directory**: The `actors/` directory contains modules defining various actors responsible for different functionalities within the Redis server. These include configuration management, information retrieval, message handling, request processing, replication, and set operations, among others. Each actor encapsulates specific responsibilities, ensuring a clean separation of concerns.

- **Handlers Directory**: The `handlers/` directory contains modules responsible for handling incoming requests from clients. These handlers are responsible for parsing and validating client commands, dispatching them to the appropriate actors, and returning responses back to the client.
  
## Communication

- **Messaging Passing**: Actors communicate through asynchronous channels, specifically using Tokio's `mpsc` (multi-producer, single-consumer) channels. Messages are defined in a structured format, often as enums, to ensure type safety and clarity in communication patterns.


## Implementation Details

- **Asynchronous Runtime**: Utilizes Tokio's asynchronous runtime to manage tasks and futures, enabling efficient execution of concurrent operations without blocking threads. This is crucial for a high-performance server like Redis, where minimizing latency is essential.

- **Command Processing**: Commands from clients are parsed and dispatched to appropriate actors based on their type. For instance, a `SET` command would be handled by the `SetCommandActor`, which processes the command and updates the internal state accordingly.

- **State Management**: State changes, such as updating key-values or deleting entries, are managed within the confines of individual actors, ensuring thread-safety without traditional locking mechanisms. This encapsulation simplifies reasoning about state consistency and concurrency issues.

## Advantages

- **Scalability**: The actor model naturally supports scalability, as actors can be distributed across multiple threads or even machines, making it suitable for a high-load environment like Redis.
- **Fault Isolation**: Since actors operate independently, failures in one part of the system are contained, enhancing the overall reliability of the service.
- **Maintainability**: Clear boundaries between actors simplify maintenance and extension of the codebase, as modifications to one actor have minimal impact on others.

## Challenges and Solutions

- **Complexity in Coordination**: While beneficial, coordinating between actors can introduce complexity, especially for operations requiring consensus or shared state. This project mitigates this through careful design of message protocols and leveraging Tokio's synchronization primitives.
- **Performance Overheads**: Message passing introduces overhead compared to shared-memory models. However, the benefits in terms of scalability and fault tolerance often outweigh these costs, especially in networked applications like Redis.

## Conclusion

The actor-based implementation eliminates `Mutex`-based concurrency issues by leveraging Rust's type system and Tokio's asynchronous runtime, achieving a high-performance, reliable Redis reimplementation.

# Redis Commands
## Set command
The `set_command.rs` file is responsible for implementing the functionality related to the Redis `SET`. It defines a `SetCommandActorHandle` struct and its associated methods to interact with a `SetCommandActor` through asynchronous messaging. The key features and functionalities include:

- **Struct Definition**: `SetCommandActorHandle` contains a `sender` field of type `mpsc::Sender<SetActorMessage>`, which is used to communicate with the `SetCommandActor`.
- **Constructor**: The `new` method initializes a `SetCommandActorHandle` by creating a channel and spawning a Tokio task that runs the actor asynchronously.
- **GET Command**: Implements the Redis `GET` command through the `get_value` method, allowing retrieval of values associated with keys. It sends a `GetValue` message to the actor and awaits a response.
- **KEYS Command**: Implements the Redis `KEYS` command via the `get_keys` method, enabling pattern-based key retrieval. It sends a `GetKeys` message to the actor and awaits a list of matching keys.
- **SET Command**: Provides functionality to set key-value pairs using the `set_value` method. It constructs a `SetValue` message and sends it to the actor, also handling expiration parameters through a separate channel (`expire_tx`).
- **DELETE Command**: Supports immediate deletion of keys with the `delete_value` method, sending a `DeleteValue` message to the actor for removal.

This module leverages Tokio's asynchronous runtime and messaging passing for non-blocking communication between components, adhering to the actor model for concurrency.

## Config command
The `config_command.rs` file is responsible for implementing the functionality related to the Redis `CONFIG` command. It defines a `ConfigCommandActorHandle` struct and its associated methods to interact with a `ConfigCommandActor` through asynchronous messaging. The key features and functionalities include:

- **Struct Definition**: `ConfigCommandActorHandle` contains a `sender` field of type `mpsc::Sender<ConfigActorMessage>`, which is used to communicate with the `ConfigCommandActor`.
- **Constructor**: The `new` method initializes a `ConfigCommandActorHandle` by creating a channel and spawning a Tokio task that runs the actor asynchronously.
- **GET Command**: Implements the Redis `GET` command through the `get_value` method, allowing retrieval of configuration parameters. It sends a `ConfigActorMessage::GetConfigValue` message to the actor and awaits a response.
- **SET Command**: Provides functionality to set configuration parameters using the `set_value` method. It constructs a `ConfigActorMessage::SetConfigValue` message and sends it to the actor, also handling expiration parameters through a separate channel (`expire_tx`).
- **LOAD CONFIG Command**: Supports loading configuration parameters from a file using the `load_config` method. It constructs a `ConfigActorMessage::LoadConfig` message and sends it to the actor, also handling expiration parameters through a separate channel (`expire_tx`).

## Info Command
The `info_command.rs` file is responsible for implementing the functionality related to the Redis `INFO` command. It defines a `InfoCommandActorHandle` struct and its associated methods to interact with a `InfoCommandActor` through asynchronous messaging. The key features and functionalities include:
- **Struct Definition**: `InfoCommandActorHandle` contains a `sender` field of type `mpsc::Sender<InfoActorMessage>`, which is used to communicate with the `InfoCommandActor`.
- **Constructor**: The `new` method initializes a `InfoCommandActorHandle` by creating a channel and spawning a Tokio task that runs the actor asynchronously.
- **GET Command**: Implements the Redis `GET` command through the `get_value` method, allowing retrieval of information. It sends a `InfoActorMessage::GetInfoValue` message to the actor and awaits a response.
- **SET Command**: Provides functionality to set information using the `set_value` method. It constructs a `InfoActorMessage::SetInfoValue` message and sends it to the actor.
