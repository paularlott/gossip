# Tagged Messages Example

This example demonstrates tag-based message routing in the gossip cluster. Nodes can be tagged with one or more tags, and messages can be sent to only those nodes that have a specific tag.

## Use Case

This feature is useful for creating logical sub-clusters within a larger gossip cluster. For example:
- Separating metadata servers from data servers
- Creating role-based message groups (e.g., "database", "cache", "compute")
- Implementing multi-tenant architectures where messages are scoped to specific tenants

## How It Works

1. **Node Tags**: Each node can have zero or more tags (strings)
2. **Tagged Messages**: Messages can optionally include a tag
3. **Routing**:
   - Tagged messages are only processed by nodes that have the matching tag
   - Tagged messages are only forwarded to nodes with the matching tag
   - Untagged messages work as before (sent to all nodes)

## Running the Example

### Scenario: 3 nodes with different tag combinations

**Terminal 1 - Node 1** (has tags: taga, tagb)
```bash
go run examples/tagged/main.go -port 8001 -tags taga,tagb
```

**Terminal 2 - Node 2** (has tags: taga, tagb)
```bash
go run examples/tagged/main.go -port 8002 -tags taga,tagb -peers 127.0.0.1:8001
```

**Terminal 3 - Node 3** (has tags: taga, tagb, tagc)
```bash
go run examples/tagged/main.go -port 8003 -tags taga,tagb,tagc -peers 127.0.0.1:8001
```

## Testing Tag-Based Routing

Once all three nodes are running:

### Test 1: Message to 'taga' (all 3 nodes receive)
In any terminal:
```
> send taga Hello to all nodes with taga
```
All three nodes should receive this message.

### Test 2: Message to 'tagc' (only Node 3 receives)
In any terminal:
```
> send tagc This is only for tagc nodes
```
Only Node 3 should receive and process this message.

### Test 3: List nodes by tag
```
> nodes tagc
```
Should show only Node 3.

```
> nodes taga
```
Should show all three nodes.

## Commands

- `send <tag> <message>` - Send a tagged message to nodes with the specified tag (message can contain spaces)
- `nodes` - List all alive nodes with their tags
- `nodes <tag>` - List only nodes that have the specified tag
- `quit` - Exit the program

## Example Commands

```
> send tagc Hello only to tagc nodes
> send taga This is a multi-word message for taga nodes
> nodes
> nodes tagc
> quit
```

## Expected Behavior

- Messages with tag 'taga' or 'tagb' will be received by all 3 nodes
- Messages with tag 'tagc' will only be received by Node 3
- Each node will only forward messages to other nodes that have the matching tag
- Untagged messages (using regular `Send()`) would be received by all nodes

## Implementation Notes

- Tags are assigned at cluster creation via `Config.Tags`
- `SendTagged(tag, msgType, data)` and `SendTaggedReliable(tag, msgType, data)` are used for tagged messages
- `GetNodesByTag(tag)` returns all alive nodes with a specific tag
- Tag matching is exact (case-sensitive)
- The packet tag field uses `omitempty` to avoid overhead for untagged messages
