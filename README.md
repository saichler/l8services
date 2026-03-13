# Layer 8 Services

A Go-based distributed services framework providing service orchestration, distributed caching, leader election, transaction management, data import/export, and file storage.

## Overview

Layer 8 Services is the core services framework for the Layer 8 platform. It provides:

- **Service Management**: SLA-based registration, activation, lifecycle management, and leader election with bully algorithm
- **Distributed Cache**: Thread-safe caching with persistent storage, notifications, and query-based retrieval
- **Map-Reduce**: Distributed parallel processing across service nodes with automatic result merging
- **Transactions**: ACID 2-phase commit across distributed services with a 6-state machine
- **Replication**: Cross-node data synchronization with deterministic round-robin load balancing
- **Data Import**: AI-assisted field mapping, heuristic matching, file parsing, and record building
- **CSV Export**: Generic CSV export with custom formatting
- **File Storage**: Upload/download with configurable storage backend
- **Leader Election**: Bully algorithm with heartbeat monitoring, election debouncing, and service grouping

## Architecture

```
go/services/
├── base/            - Foundation CRUD service handler with Before/After callbacks
├── csvexport/       - CSV export service with formatting
├── dataimport/      - Data import pipeline (AI mapping, parsing, transformation)
├── dcache/          - Distributed cache with notifications and persistence
├── filestore/       - File upload/download management
├── manager/         - Service orchestration, leader election, MapReduce
├── recovery/        - Data synchronization from leader to followers
├── replication/     - Replication index tracking (key-to-node mapping)
└── transaction/     - ACID 2-phase commit
    ├── states/      - State machine: Create, Queue, Run, Commit, Rollback, Cleanup
    └── requests/    - Transaction request types
```

### Core Components

**Base Service** (`services/base/`) - Foundation CRUD handler implementing Post, Put, Patch, Delete, and Get with integrated cache interaction, SLA-based Before/After callbacks, query support with pagination, and an async notification queue (capacity: 50,000).

**Distributed Cache** (`services/dcache/`) - Thread-safe distributed cache supporting persistent storage backends, property change listeners, paginated fetch, metadata collection, and replication-aware operations.

**Service Manager** (`services/manager/`) - Central orchestrator handling service registration and discovery, request routing, transaction coordination, leader election (bully algorithm with 3s election timeout, 5s heartbeat timeout), election debouncing (500ms window to prevent storms), participant registry with UUID-based round-robin, and service grouping with dynamic resolver.

**Transaction Engine** (`services/transaction/`) - 6-state machine (T01 Create -> T02 Queue -> T03 Run -> T04 Commit / T05 Rollback -> T06 Cleanup) with optimized rollback (only targets peers that committed) and proper PreCommit map cleanup.

**Data Import** (`services/dataimport/`) - Full import pipeline with four sub-handlers: AI-based field mapping (ImprtAI), import execution with validation (ImprtExec), model metadata queries (ImprtInfo), and template transfer/export (ImprtXfer). Includes heuristic field matching, multi-format file parsing, value transformation, and typed record building.

**CSV Export** (`services/csvexport/`) - Stateless export service accepting POST requests and returning formatted CSV data.

**File Store** (`services/filestore/`) - File upload (POST) and download (PUT) with configurable size limits (default 5MB) and storage root (`/data/l8files`).

**Replication** (`services/replication/`) - Tracks which nodes store which data elements via L8ReplicationIndex, mapping service keys to node UUIDs and replica numbers.

**Recovery** (`services/recovery/`) - Synchronizes data from leader to followers in pages (500 elements/page). Currently disabled pending further testing.

## Quick Start

```go
package main

import (
    "github.com/saichler/l8services/go/services/manager"
    "github.com/saichler/l8services/go/services/dcache"
)

func main() {
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
}
```

## Usage

### Distributed Cache

```go
import "github.com/saichler/l8services/go/services/dcache"

// Basic cache
cache := dcache.NewDistributedCache(
    "myService", 1, "MyModel", "source1",
    listener, resources,
)

// Cache with persistent storage
cache := dcache.NewDistributedCacheWithStorage(
    "myService", 1, "MyModel", "source1",
    listener, resources, storage,
)
```

### Service Management

```go
import "github.com/saichler/l8services/go/services/manager"

services := manager.NewServices(resources)

sla := ifs.NewServiceLevelAgreement()
sla.SetServiceName("MyService")
sla.SetServiceArea(1)
sla.SetServiceHandlerInstance(myHandler)

handler, err := services.Activate(sla, vnic)
result := services.Handle(payload, action, vnic, message)
```

### Map-Reduce

```go
// Implement the IMapReduceService interface
type MyService struct{}

func (s *MyService) Merge(results map[string]ifs.IElements) ifs.IElements {
    // Merge results from all nodes
    return merged
}

// Execute across all participants
results := serviceManager.MapReduce(handler, payload, ifs.MapR_GET, message, vnic)
```

Supported actions: `MapR_POST`, `MapR_GET`, `MapR_PUT`, `MapR_PATCH`, `MapR_DELETE`

## API Reference

### Service Handler Interface (`IServiceHandler`)

- `Post(payload, vnic)` - Create resources
- `Put(payload, vnic)` - Replace resources
- `Patch(payload, vnic)` - Update resources
- `Delete(payload, vnic)` - Remove resources
- `Get(payload, vnic)` - Retrieve resources
- `Failed(payload, vnic, message)` - Handle failures

### Distributed Cache Operations

- `Post()` / `Put()` / `Patch()` / `Delete()` - CRUD operations
- `Get()` - Single element lookup
- `Fetch()` - Paginated query-based retrieval
- `Collect()` - Bulk filtered retrieval
- `Metadata()` - Cache metadata and counts

### Leader Election Messages

- `ElectionRequest` / `ElectionResponse` - Election protocol
- `LeaderAnnouncement` / `LeaderHeartbeat` - Leader lifecycle
- `LeaderQuery` / `LeaderResign` / `LeaderChallenge` - Leader management

### Participant Registry

- `ServiceRegister` / `ServiceUnregister` - Registration
- `ServiceQuery` - Discovery
- Group resolver for logical service grouping

## Dependencies

| Dependency | Purpose |
|-----------|---------|
| `l8bus` | Network overlay and virtual networking |
| `l8types` | Interface definitions (`IServices`, `IServiceHandler`, `IDistributedCache`, `IVNic`) |
| `l8utils` | Cache, queue, and utility data structures |
| `l8srlz` | Serialization framework |
| `l8reflect` | Introspection and type reflection |
| `l8ql` | Query language support |
| `l8test` | Testing infrastructure (multi-node topology) |

## Testing

```bash
cd go
go test ./tests/...
```

Tests use the `l8test` framework with a 4-node topology across 3 VNets. Test coverage includes:

- Base service CRUD operations
- Distributed cache operations
- Map-Reduce with 9 participants
- Transaction coordination (sync and async)
- Replication counting
- Service point deactivation
- Notification delivery
- Service manager operations

Generate coverage report:

```bash
cd go && ./test.sh
# Open cover.html in browser
```

## Project Structure

```
l8services/
├── go/
│   ├── services/
│   │   ├── base/            # CRUD service foundation (4 files)
│   │   ├── csvexport/       # CSV export (4 files)
│   │   ├── dataimport/      # Data import pipeline (9 files)
│   │   ├── dcache/          # Distributed cache (10 files)
│   │   ├── filestore/       # File storage (5 files)
│   │   ├── manager/         # Service orchestration (10 files)
│   │   ├── recovery/        # Data recovery (1 file)
│   │   ├── replication/     # Replication tracking (1 file)
│   │   └── transaction/     # ACID transactions (10 files)
│   │       ├── states/      # 6-state machine
│   │       └── requests/    # Request types
│   ├── tests/               # 12 test files (~1,540 lines)
│   └── vendor/              # Vendored dependencies
└── README.md
```

~5,100 lines of source code across 54 Go files, all under 500 lines each.

## License

This project is part of the Layer 8 ecosystem. Please refer to the main Layer8 project for licensing information.
