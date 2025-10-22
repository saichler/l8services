# Layer 8 Services, API, Concurrency & Caching

A comprehensive Go-based distributed services framework providing distributed caching, transaction management, and service orchestration capabilities.

## Overview

Layer 8 Services is a high-performance distributed services framework built on the Layer8 platform that provides enterprise-grade capabilities for modern distributed applications:

- **Advanced Distributed Cache**: Thread-safe, synchronized distributed caching with persistent storage support and optimized data fetching
- **Enhanced Transaction Management**: ACID transaction support across distributed services with improved 2-phase commit protocol and optimized state management
- **Service Management**: Registration, activation, and lifecycle management of microservices with SLA-based architecture and improved error handling
- **Replication Services**: Data replication and synchronization across service instances with deterministic round-robin load balancing
- **Notification System**: Event-driven notifications for cache updates and service state changes
- **Performance Optimizations**: Recent improvements in service activation, error recovery, and transaction rollback mechanisms

## Architecture

The framework is organized into several core components:

### Core Services

- **DCache** (`services/dcache/`): Distributed caching with thread-safe operations, persistence, and notifications
- **Transaction Manager** (`services/transaction/`): Distributed transaction coordination with full ACID properties and optimized state management
- **Service Manager** (`services/manager/`): Service lifecycle management and request routing
- **Replication Service** (`services/replication/`): Data synchronization across distributed nodes

### Key Features

- **Thread-Safe Operations**: All cache and service operations are protected with appropriate locking mechanisms
- **Optimized Transaction Processing**: Recently refactored transaction system with improved state management and coordination
- **Enhanced Store Operations**: Improved store loading mechanisms with better error handling and performance
- **Advanced Sorting Algorithms**: Optimized sorting operations for better data organization and retrieval
- **Improved Fetch Operations**: Enhanced data fetching with better reliability and performance
- **IP Comparison Utilities**: Advanced networking utilities for distributed service coordination
- **Persistence Layer**: Optional storage backend for cache durability with improved reliability
- **Event Notifications**: Real-time notifications for data changes and service events
- **Security Integration**: Built-in security checks for all service operations
- **Health Monitoring**: Service health tracking and reporting with enhanced service point deactivation
- **Web Service Integration**: RESTful API endpoints for external service interaction

## Recent Updates (October 2025)

### Service Activation Improvements
- **SLA-Based Architecture**: Refactored service activation to use Service Level Agreements (SLA) for better service management
- **Enhanced Error Handling**: Fixed panic conditions on nil responses from handlers
- **Improved Error Recovery**: Better error propagation and handling during service activation
- **Graceful Failure Handling**: Services now continue activation even if non-critical operations fail

### Transaction System Enhancements
- **Optimized Rollback Logic**: Only sends rollback to peers that successfully committed
- **Memory Management**: Fixed memory leaks in PreCommit map with proper cleanup on all paths
- **Deterministic Round-Robin**: Replaced index-based approach with UUID tracking for true round-robin distribution
- **2-Phase Commit Improvements**: Better handling of edge cases in distributed transactions

### Performance & Stability Fixes
- **Crash Prevention**: Fixed multiple crash scenarios in service activation and handler responses
- **Test Coverage**: Improved test reliability and coverage reporting
- **Resource Cleanup**: Enhanced cleanup mechanisms for transaction states and service deactivation

### Known Issues & Future Work
- **Single-threaded Transaction Processing**: Currently processes transactions sequentially per service (optimization planned)
- **Concurrent GET Operations**: Implementation for concurrent reads during long transactions (in progress)
- **Graceful Shutdown**: Improved resource lifecycle management and goroutine cleanup (planned)

## Installation

```bash
go mod download
```

## Quick Start

```go
package main

import (
    "github.com/saichler/l8services/go/services/manager"
    "github.com/saichler/l8services/go/services/dcache"
    // Import your Layer8 dependencies
)

func main() {
    // Initialize resources and VNIC
    resources := // ... initialize Layer8 resources
    vnic := // ... initialize VNIC

    // Create service manager
    serviceManager := manager.NewServices(resources)

    // Setup a distributed cache
    cache := dcache.NewDistributedCache(
        "CacheService", 1, "MyDataModel", "source1",
        cacheListener, resources,
    )

    // Create and activate a service with SLA
    sla := ifs.NewServiceLevelAgreement()
    sla.SetServiceName("MyService")
    sla.SetServiceArea(1)
    sla.SetServiceHandlerInstance(myServiceHandler)

    handler, err := serviceManager.Activate(sla, vnic)
    if err != nil {
        log.Fatal("Failed to activate service:", err)
    }

    // Service is now ready to handle requests
}
```

## Dependencies

- **Layer8**: Core networking and overlay infrastructure
- **L8Types**: Type definitions and interfaces
- **L8Utils**: Utility functions and data structures
- **L8Srlz**: Serialization framework
- **L8Test**: Testing infrastructure
- **Reflect**: Advanced reflection utilities

## Usage

### Creating a Distributed Cache

```go
import "github.com/saichler/l8services/go/services/dcache"

// Create a new distributed cache
cache := dcache.NewDistributedCache(
    "myService",           // service name
    1,                     // service area
    "MyModel",            // model type
    "source1",            // source identifier
    listener,             // cache listener
    resources,            // system resources
)

// With persistent storage
cache := dcache.NewDistributedCacheWithStorage(
    "myService", 1, "MyModel", "source1",
    listener, resources, storage,
)
```

### Service Management

```go
import "github.com/saichler/l8services/go/services/manager"

// Create service manager
services := manager.NewServices(resources)

// Create Service Level Agreement for service activation
sla := ifs.NewServiceLevelAgreement()
sla.SetServiceName("MyService")
sla.SetServiceArea(1)
sla.SetServiceHandlerInstance(myHandler)

// Activate service with SLA
handler, err := services.Activate(sla, vnic)

// Handle service requests
result := services.Handle(payload, action, vnic, message)
```

### Transaction Management

```go
// Transactions are automatically managed by the framework
// with optimized state management and coordination
// The system supports:
// - Create: Initialize new transaction with state tracking
// - Run: Execute transaction with replication support
// - Lock: Acquire distributed locks across services
// - Commit: Commit changes across all participants
// - Rollback: Rollback changes on failure with state cleanup
// 
// Recent optimizations include:
// - Improved state management architecture
// - Enhanced transaction coordination
// - Better resource cleanup and lifecycle management
// - Optimized store loading with enhanced error handling
// - Improved sorting algorithms for better performance
// - Enhanced fetch operations with better reliability
// - Advanced IP comparison utilities for network coordination
```

## Testing

Run the test suite:

```bash
cd go
go test ./tests/...
```

Generate test coverage:

```bash
./test.sh
```

View coverage report:
- Open `go/cover.html` in your browser

## Project Structure

```
l8services/
├── go/
│   ├── services/
│   │   ├── dcache/          # Distributed caching implementation
│   │   ├── manager/         # Service lifecycle management with SLA
│   │   ├── replication/     # Data replication services
│   │   ├── recovery/        # Service recovery mechanisms
│   │   ├── base/            # Base service implementations
│   │   └── transaction/     # Transaction management
│   │       ├── states/      # Transaction state management (refactored)
│   │       └── requests/    # Transaction request handling
│   ├── tests/               # Comprehensive test suite
│   ├── vendor/              # Vendored dependencies
│   ├── go.mod              # Go module definition
│   ├── go.sum              # Go module checksums
│   ├── cover.html          # Test coverage report
│   └── test.sh             # Test execution script
├── services.md             # Performance & concurrency analysis document
├── todo.txt               # Development tasks and improvements tracking
├── web.html               # Web service interface
└── README.md              # This file
```

## API Reference

### Distributed Cache Operations

- `Put(key, value)`: Store value in cache
- `Get(key)`: Retrieve value from cache
- `Delete(key)`: Remove value from cache
- `Update(key, value)`: Update existing value
- `Sync()`: Synchronize cache with peers
- `Collect(filter)`: Bulk retrieve filtered items

### Service Handler Interface

Services must implement the `IServiceHandler` interface:

- `Post(payload, vnic)`: Create new resources
- `Put(payload, vnic)`: Replace existing resources
- `Patch(payload, vnic)`: Update existing resources
- `Delete(payload, vnic)`: Remove resources
- `Get(payload, vnic)`: Retrieve resources
- `Failed(payload, vnic, message)`: Handle failure scenarios

## Documentation

### Performance Analysis
A comprehensive performance and concurrency analysis is available in `services.md` which covers:
- Memory management patterns and optimization opportunities
- Concurrency bottlenecks and synchronization issues
- Transaction processing architecture recommendations
- Resource lifecycle management improvements
- Critical metrics and monitoring recommendations

### Todo Tracking
Active development tasks and improvements are tracked in `todo.txt` with:
- High priority fixes for transaction mechanism
- Medium priority performance enhancements
- Future architectural improvements
- Current implementation status for each item

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes with tests
4. Ensure all tests pass
5. Submit a pull request

## License

This project is part of the Layer8 ecosystem. Please refer to the main Layer8 project for licensing information.

## Support

For questions and support, please refer to the Layer8 community resources or create an issue in the repository.
