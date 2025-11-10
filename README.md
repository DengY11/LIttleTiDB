# MyTiDB

Inspired by the architectural elegance of TiDB, this project is a hands-on journey to build a distributed, horizontally-scalable, NewSQL database from scratch in Go. It serves as a practical exploration of the core concepts behind modern distributed systems.

## Core Features Implemented

- **Centralized Placement Driver (PD)**: A `pd` service, acting as the brain of the cluster, responsible for:
  - **Global ID Allocation**: Issuing unique, cluster-wide IDs for nodes and regions using Snowflake.
  - **Service Discovery**: Allowing new nodes to discover the cluster leader and join dynamically.
  - **Metadata Management**: Maintaining the state of the cluster, including store and region information.

- **Distributed Key-Value Store (KV-Server)**: A `kv-server` that functions as a "Store" node, featuring:
  - **Raft-based Replication**: Utilizes the `hashicorp/raft` library to ensure strong consistency for data within a Raft group.
  - **Dynamic Cluster Membership**: Nodes can dynamically join an existing cluster by requesting an ID from PD and joining the leader.
  - **Automatic Leader Election & Failover**: The cluster can automatically elect a new leader if the current leader fails, ensuring high availability.

## Next Steps: The Road to a True Distributed Database

With the foundational consensus and service discovery layers in place, our next major challenge is to implement **Region Split**. This will evolve the system from a single Raft group to a multi-Raft system, enabling true horizontal scalability and laying the groundwork for intelligent load balancing.