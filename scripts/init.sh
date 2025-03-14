#!/bin/bash

# Define the number of VMs
VM_COUNT=5

# Define the bootstrap node (VM 1)
BOOTSTRAP_NODE=1

# Default parameters
k=1
m=0

if [[ $# -ge 1 ]]; then
    k=$1
fi

if [[ $# -ge 2 ]]; then
    m=$2
fi

echo "Using parameters: k=$k, m=$m"

for i in $(seq 1 $VM_COUNT); do
  if [ $i -eq $BOOTSTRAP_NODE ]; then
    # If it's the bootstrap node, run the bootstrap command and a regular node on VM$i
    ssh team_17-vm$i "source /home/ubuntu/.cargo/env && cd /home/ubuntu/Chord-DHT && cargo run --release bootstrap $k $m" & 
    echo "Starting bootstrap node on VM$i"
    
    # Run second node on the same VM with a different port
    ssh team_17-vm$i "source /home/ubuntu/.cargo/env && cd /home/ubuntu/Chord-DHT && cargo run --release node 1" &
    echo "Starting second node on VM$i"
  else
    # For regular nodes, run two nodes with different ports (e.g., 8001 and 8002)
    ssh team_17-vm$i "source /home/ubuntu/.cargo/env && cd /home/ubuntu/Chord-DHT && cargo run --release node 1" &
    echo "Starting first node on VM$i"

    ssh team_17-vm$i "source /home/ubuntu/.cargo/env && cd /home/ubuntu/Chord-DHT && cargo run --release node 2" &
    echo "Starting second node on VM$i"
  fi
done



echo "Finished starting all nodes"
