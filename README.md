# Chord-DHT

To build the project download the root of this repository and perform:
```
cd Chord-DHT
cargo build
```

**1.** To start the bootstrap node server, type into the current terminal: 
```
cargo run --release bootsrtap <REPLICA_FACTOR> <CONSISTENCY>
```
- REPLICA_FACTOR : defines the number of copies for each item in the key-value store in neighbouring nodes and must be > 0.
- CONSISTENCY supports 2 models : 0 => Enevtual, 1 => Chain Replication


**2.** To start a peer node server, open a new terminal and type:
```
cargo run --release node <NUM_NODE>
```
Repeat this to create more peer nodes in the network. Note that <NUM_NODE> denotes the listening port of the server. Two nodes can have same <NUM_NODE> parameter only if they are using unique IP addresses. 

**3.** Finally to start the cli, open a new terminal again and type:
```
cargo run --release cli <PEER IP> <PEER PORT> <COMMAND> <ARGS> 
```
Or just simply try:
```
cargo run --release cli help
```
to inspect available options

