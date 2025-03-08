# Chord-DHT

To build the project:
```
cd Chord-DHT
cargo build
```

**1.** To start the bootstrap node server, type into the current terminal: 
```
cargo run bootsrtap <REPLICA_FACTOR> <CONSISTENCY>
```

**2.** To start a peer node server, open a new terminal and type:
```
cargo run node
```
Repeat this to create more peer nodes in the network. 

**3.** Finally to start the cli, open a new terminal again and type:
```
cargo run cli
```

