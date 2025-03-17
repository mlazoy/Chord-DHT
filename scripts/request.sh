#!/bin/bash

DATA_DIR="../data/requests/"
# Define the IP addresses and ports
declare -A IP_PORT_MAP
IP_PORT_MAP=(
    ["10.0.24.44:8000"]="requests_00.txt"
    ["10.0.24.44:8001"]="requests_01.txt"
    ["10.0.24.219:8001"]="requests_02.txt"
    ["10.0.24.219:8002"]="requests_03.txt"
    ["10.0.24.212:8001"]="requests_04.txt"
    ["10.0.24.212:8002"]="requests_05.txt"
    ["10.0.24.124:8001"]="requests_06.txt"
    ["10.0.24.124:8002"]="requests_07.txt"
    ["10.0.24.206:8001"]="requests_08.txt"
    ["10.0.24.206:8002"]="requests_09.txt"
)

# Function to get the correct requests file for the IP and port combination
get_requests_file() {
    local ip=$1
    local port=$2
    local key="${ip}:${port}"
    echo "$DATA_DIR${IP_PORT_MAP[$key]}"
}

echo Building the client...
for key in "${!IP_PORT_MAP[@]}"; do
    ip=$(echo "$key" | cut -d ':' -f 1)
    port=$(echo "$key" | cut -d ':' -f 2)
    target_dir="target_${port}"
    CARGO_TARGET_DIR="$target_dir" 
    cargo build --release --target-dir "$target_dir"  # Build the project
done


for key in "${!IP_PORT_MAP[@]}"; do
    ip=$(echo "$key" | cut -d ':' -f 1)
    port=$(echo "$key" | cut -d ':' -f 2)
    target_dir="target_${port}"
    CARGO_TARGET_DIR="$target_dir"  
    log_file="outputs/output_${ip}_${port}.log"
    
    cargo run --release --target-dir "$target_dir" cli "$ip" "$port" requests $(get_requests_file "$ip" "$port") > "$log_file" &
done


# Wait for all background processes to finish
wait
echo "✅ Finished requestsing data on all VMs."