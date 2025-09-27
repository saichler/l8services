# Layer 8 Services, API, Concurrency & Caching

A comprehensive Go-based distributed services framework providing distributed caching, transaction management, and service orchestration capabilities.

## Overview

Layer 8 Services is a high-performance distributed services framework built on the Layer8 platform that provides enterprise-grade capabilities for modern distributed applications:

- **Advanced Distributed Cache**: Thread-safe, synchronized distributed caching with persistent storage support and optimized data fetching
- **Enhanced Transaction Management**: ACID transaction support across distributed services with improved 2-phase commit protocol and optimized state management
- **Service Management**: Registration, activation, and lifecycle management of microservices with enhanced service point handling
- **Replication Services**: Data replication and synchronization across service instances with improved counting and sorting mechanisms
- **Notification System**: Event-driven notifications for cache updates and service state changes
- **Performance Optimizations**: Recent improvements in store loading, sorting algorithms, and fetch operations

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

## Installation

```bash
go mod download
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

// Register a service handler
services.RegisterServiceHandlerType(myHandler)

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
go/
├── services/
│   ├── dcache/          # Distributed caching implementation
│   ├── manager/         # Service lifecycle management
│   ├── replication/     # Data replication services
│   └── transaction/     # Transaction management
│       ├── states/      # Transaction state management (refactored)
│       └── requests/    # Transaction request handling
├── tests/               # Comprehensive test suite
├── vendor/              # Vendored dependencies
├── go.mod              # Go module definition
└── test.sh             # Test execution script
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
