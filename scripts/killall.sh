#!/bin/bash

# Define the number of VMs
VM_COUNT=5

VM_PREFIX="team_17-vm"

for i in $(seq 1 $VM_COUNT); do
  echo "Stopping processes on VM$i..."
  ssh "$VM_PREFIX$i" "pkill -f 'cargo run'"

  echo "Stopped all processes on VM$i"
done

echo "Finished stopping all nodes on all VMs"
