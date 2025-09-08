# Health Monitoring Example

This example demonstrates the health monitoring capabilities of the gossip library, showing how nodes track each other's health and handle failures.

## Features Demonstrated

- **Automatic Health Checking**: Nodes periodically ping each other to verify they're alive
- **Suspect State Management**: Nodes that don't respond are marked as suspect and retried
- **Dead Node Detection**: Suspect nodes that remain unresponsive are marked as dead
- **Target Node ID Validation**: Messages include target node IDs to prevent processing by wrong nodes
- **Real-time Status Updates**: Live display of cluster health and node state changes

## Configuration

The example uses faster intervals than production defaults for demonstration:

- **Health Check Interval**: 2 seconds (vs 2s default)
- **Suspect Timeout**: 1 second (vs 1.5s default) 
- **Suspect Retry Interval**: 500ms (vs 1s default)
- **Dead Node Timeout**: 5 seconds (vs 15s default)

## Running the Example

Start the first node:
```bash
go run examples/health/main.go -bind 127.0.0.1:8000
```

Start additional nodes:
```bash
go run examples/health/main.go -bind 127.0.0.1:8001 -peers 127.0.0.1:8000
go run examples/health/main.go -bind 127.0.0.1:8002 -peers 127.0.0.1:8000
```

## Testing Health Monitoring

1. **Normal Operation**: Watch nodes join and maintain healthy status
2. **Network Partition**: Kill a node process and observe state transitions:
   - Node becomes suspect after 1 second of no response
   - Node marked as dead after 5 seconds of being suspect
3. **Recovery**: Restart a killed node and watch it rejoin as alive

## Expected Output

```
Health monitoring demo started on 127.0.0.1:8000
Node ID: 01960f9b-72ca-7a51-9efa-47c12f42a138
Health check interval: 2s
Suspect timeout: 1s

[15:04:05] Cluster status: 1 alive, 0 suspect, 0 dead

[15:04:15] Node a1b2c3d4: Unknown -> Alive
[15:04:15] Cluster status: 2 alive, 0 suspect, 0 dead
  Nodes:
    a1b2c3d4: Alive

[15:04:25] Node a1b2c3d4: Alive -> Suspect
[15:04:25] Cluster status: 1 alive, 1 suspect, 0 dead
  Nodes:
    a1b2c3d4: Suspect

[15:04:30] Node a1b2c3d4: Suspect -> Dead
[15:04:30] Cluster status: 1 alive, 0 suspect, 1 dead
  Nodes:
    a1b2c3d4: Dead
```

## Implementation Details

The health monitoring system uses:

- **Worker Pool**: Configurable number of workers handle health checks concurrently
- **Task Queue**: Health check tasks are queued to prevent blocking
- **Direct Pings**: Active health checks using ping/pong messages with target node validation
- **State Transitions**: Automatic progression from Alive → Suspect → Dead based on timeouts