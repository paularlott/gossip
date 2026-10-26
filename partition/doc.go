// Package partition replicates partitioned data over gossip membership with a
// single owner per partition and majority-committed writes.
//
// The data is split into a fixed number of partitions (Config.Partitions). Each
// partition has at most one owner at a time. The nodes that are ready are ranked
// per partition by rendezvous hashing: rank 0 owns the partition when it can,
// and each next rank takes over after a further Config.RankDelay if the
// partition has no owner; an owner is never asked to step down by rank, but a
// rank 0 node that returns claims its partitions from the lower ranks that
// hold them. A change is sent to the owner, which checks it against the
// partition's committed data (Store.Prepare), stores it as a pending write on
// itself and a majority of the configured cluster size (Config.ClusterSize,
// never the number of nodes currently visible) and only then commits it. Write
// returns the write's position and the reply Prepare made for the caller;
// WaitApplied waits until this node has applied a write. A new owner takes
// over with a promise round, in the manner of Paxos: it adopts the highest-epoch pending write left by its predecessor
// and settles it before accepting new changes. Safety is preferred over
// liveness throughout: when the package is unsure, it refuses.
//
// What a partition holds is opaque to the package. The application supplies
// the durable side through the Store interface (promises, pending and
// committed writes, snapshots for catch-up) and the package supplies the
// protocol: elections of owners, replication, heartbeats, catch-up streams,
// startup synchronisation and the bounded-staleness read rule (CanRead). Manager.Read
// is the linearizable read: it runs Store.Read on the owner once the owner has confirmed
// with a majority that it still owns the partition.
//
// Catch-up uses the library's streams, so the cluster's transport must support them (the socket and HTTP transports
// do). A Manager must be Started on every node of the cluster with the same Partitions and ClusterSize; nodes that
// disagree on ClusterSize ignore each other.
//
// The protocol's message types are allocated in the library's registry
// (gossip.PartHeartbeatMsg ... gossip.PartReadMsg).
package partition
