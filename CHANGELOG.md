# Changelog

## [1.0.1] - 2024-10-13 [Release]

### Added

- Added `WorkerPoolInterface::setPoolContext()` + `WorkerInterface::getPoolContext()` methods. 
Each `Worker` can receive a shared application context during startup, 
which is set using the `setPoolContext()` method before calling the `WorkerPoolInterface::start()` method.


## [1.0.0] - 2024-07-13 [Release]

### Fixed

- Fixed socket waiting when the server is about to shut down. 
The issue remains for `UNIX` and is caused by the architecture of the `AMPHP` `httpserver`.
- Fixed the synchronization of `WorkerState` for the isReady status and fields related to the worker's termination state.
- Fixed the bug with Server Socket hanging under `Windows`.
- Fixed bug SHARED MEMORY delete for `Worker` instances.

### Added

- Added a component for monitoring the summary application state: `start time`, `number of restarts`, `uptime`.
- Added a new strategy `AutoRestartByQuota`, 
which allows you to restart the worker after a certain number of processed jobs or memory quota, etc.

### Changed

- Refined the FLOW of error analysis for the running Worker process.
- Refactored the `WorkerProcessContext` flow. Coroutines for monitoring the process and the message queue 
are separated by their own triggers, which ensure a clear order of execution. 
The message loop coroutine finishes first, followed by the observer coroutine.
- Added a method `Worker::awaitShutdown`, which ensures the orderly shutdown of the worker. 
Now the worker sends a `NULL` message, signaling the closure of the channel, 
and then waits for confirmation from the parent process. Only after this does it terminate.
- Added a new method `WorkerPool::restartWorker`, which allows you to restart the worker softly without stopping the entire pool.
- Improved error handling from workers. Remote exceptions are properly accounted for and available for logging.

## [0.9.0] - 2024-07-01 [Pre-release]

### Added

- Basic architecture for a Worker Pool library
- Worker groups/scheme functionality
- Worker strategies: `Pickup`, `Restart`, `Scale`, `Runner`, `JobExecutor`, `Socket` 
- `JobIpc` communication
- `CoroutineScheduler` component for scheduling tasks
- `WorkersStorage` shared memory component 
- Windows socket transfer support for Reactor-worker.
- Prometheus metrics
