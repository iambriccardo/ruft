# Ruft - Raft Consensus Protocol in Rust

This is a personal project in which I will explore an implementation of the Raft Consensus Protocol in Rust. My goal is to learn the protocol with a simple local application and then actually extend it to be a proper implementation that can be hosted on separate machines.

The end goal is to have a framework on which you could build your distributed applications. A common use case could be to build a replicated strongly consistent key value store.

For now the implementation will be done with multiple threads locally but I plan to create an abstraction layer that will allow to plugin any kind of transport layer.

## Architecture Diagram

A simplified architecture of the system is shown below:

![Architecture Diagram](/images/architecture.png)
