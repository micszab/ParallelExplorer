# ParallelExplorer

## Task Description

The goal of this program is to concurrently process a two-dimensional array of integers to find pairs of adjacent values whose sum equals a given number. These values are then replaced with zeros.

## Program Functionality

- **Access to the array:** Only possible through the methods of the provided interface.
- **Concurrency:**
  - Data must be read by multiple threads simultaneously.
  - Only one dedicated thread can write zeros to the array.
  - Each thread receives a starting position and examines adjacent positions.
  - Reading threads must operate concurrently, and the workload should be evenly distributed.

## Implementation Requirements

- **Thread Creation:** Threads must be created using the provided factory.
- **Thread Management:**
  - The thread modifying the array (inserting zeros) should remain in a sleep state when no data is available for processing.
  - Threads must run until the entire array has been processed.
  - After completion, all threads must transition to the TERMINATED state.
- **start:**
  - Must be non-blocking.
  - Threads can only begin work after it is called.
- **result:**
  - Returns the set of detected pairs after processing is complete.
  - Must never return `null`.
- **Restrictions:**
  - The same position must not be read multiple times.
  - Threads must not go beyond the array boundaries.
  - Threads created by `Executors` are not allowed – only threads from the provided factory can be used.
