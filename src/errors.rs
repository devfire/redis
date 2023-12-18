use thiserror::Error;

/// RedisError enumerates all possible errors returned by this library.
#[derive(Error, Debug)]
pub enum RedisError {
    /// Nom parser was unable to parse the in-bound resp message
    #[error("Unable to parse message")]
    ParseFailure,

    /// Redis got an incorrect number of parameters
    #[error("Incorrect number of parameters")]
    InputFailure,

    /// Key not found
    #[error("Invalid key passed")]
    KeyNotFound,

    /// Represents all other cases of `std::io::Error`.
    #[error("Invalid digit parsing")]
    ParseIntError(#[from] std::num::ParseIntError),

    /// Represents all other cases of `std::io::Error`.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
