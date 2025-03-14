#!/bin/bash

# Define the number of VMs
VM_COUNT=5
VM_PREFIX="team_17-vm"
PORTS=(8000 8001 8002)

for i in $(seq 1 $VM_COUNT); do
    echo "Stopping processes on VM$i..."
    
    for port in "${PORTS[@]}"; do
        ssh "$VM_PREFIX$i" "pids=\$(lsof -ti :$port); if [[ -n \"\$pids\" ]]; then kill -9 \$pids; fi"
    done

    echo "Stopped all processes on VM$i"
done

echo "✅ Finished stopping all nodes on all VMs"
