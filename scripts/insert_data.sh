#!/bin/bash

DATA_DIR="../data/insert/"
# Define the IP addresses and ports
declare -A IP_PORT_MAP
IP_PORT_MAP=(
    ["10.0.24.44:8000"]="insert_00_part.txt"
    ["10.0.24.44:8001"]="insert_01_part.txt"
    ["10.0.24.219:8001"]="insert_02_part.txt"
    ["10.0.24.219:8002"]="insert_03_part.txt"
    ["10.0.24.212:8001"]="insert_04_part.txt"
    ["10.0.24.212:8002"]="insert_05_part.txt"
    ["10.0.24.124:8001"]="insert_06_part.txt"
    ["10.0.24.124:8002"]="insert_07_part.txt"
    ["10.0.24.206:8001"]="insert_08_part.txt"
    ["10.0.24.206:8002"]="insert_09_part.txt"
)


# Function to get the correct insert file for the IP and port combination
get_insert_file() {
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

start_time=$(date +%s%3N)  # Start time for the entire process

# Loop through the IP_PORT_MAP and run the insertion concurrently
for key in "${!IP_PORT_MAP[@]}"; do
    ip=$(echo "$key" | cut -d ':' -f 1)
    port=$(echo "$key" | cut -d ':' -f 2)
    target_dir="target_${port}"
    CARGO_TARGET_DIR="$target_dir"  
    cargo run --release --target-dir "$target_dir" cli "$ip" "$port" insert -f $(get_insert_file "$ip" "$port") &  # Run the insert data function in the background
done

# Wait for all background processes to finish
wait

end_time=$(date +%s%3N)
time=$((end_time - start_time))  # Total time for all nodes
# Calculate overall throughput
total_keys=500  # Adjust based on actual data
throughput=$(echo "scale=2; $total_keys / ($time / 1000)" | bc)  # Keys per second

echo "========================================"
echo "Time (Max Node Time): $time ms"
echo "Throughput: $throughput inserts/sec"
echo "✅ Finished inserting data on all VMs."