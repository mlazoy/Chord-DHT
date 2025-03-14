#!/bin/bash

#!/bin/bash

# Define the IP addresses
BOOTSTRAP_IP=("10.0.24.44")
IP_ADDRESSES=("10.0.24.219" "10.0.24.212" "10.0.24.124" "10.0.24.206")

# Define the ports
BOOT_PORTS=("8000" "8001")
PORTS=("8001" "8002")

#join for the bootsrap first
echo "Running 'cli join' on $IP for port ${BOOT_PORTS[0]}"
cargo run --release cli $BOOTSTRAP_IP ${BOOT_PORTS[0]} join
sleep 5

echo "Running 'cli join' on $IP for port ${BOOT_PORTS[1]}"
cargo run --release cli $BOOTSTRAP_IP ${BOOT_PORTS[1]} join
sleep 5

# Iterate over each IP and run the cli commands
for i in $(seq 1 ${#IP_ADDRESSES[@]}); do
  IP=${IP_ADDRESSES[$i-1]}  # Get the IP address for this VM

  # Run the first `cli` command for port 8000
  echo "Running 'cli join' on $IP for port ${PORTS[0]}"
  cargo run --release cli $IP ${PORTS[0]} join
  sleep 5

  # Run the second `cli` command for port 8001
  echo "Running 'cli join' on $IP for port ${PORTS[1]}"
  cargo run --release cli $IP ${PORTS[1]} join
  sleep 5
done

echo "Finished running CLI join commands for all nodes"
