#!/bin/bash

# Define IP addresses and ports for query nodes
declare -A QUERY_NODES
QUERY_NODES=(
    ["10.0.24.44:8000"]="query_00.txt"
    ["10.0.24.44:8001"]="query_01.txt"
    ["10.0.24.219:8001"]="query_02.txt"
    ["10.0.24.219:8002"]="query_03.txt"
    ["10.0.24.212:8001"]="query_04.txt"
    ["10.0.24.212:8002"]="query_05.txt"
    ["10.0.24.124:8001"]="query_06.txt"
    ["10.0.24.124:8002"]="query_07.txt"
    ["10.0.24.206:8001"]="query_08.txt"
    ["10.0.24.206:8002"]="query_09.txt"
)

# Function to get query file for a given node
get_query_file() {
    local ip=$1
    local port=$2
    local key="${ip}:${port}"
    echo "../data/queries/${QUERY_NODES[$key]}"
}

# Function to query keys and measure time per node
query_data() {
    local ip=$1
    local port=$2
    local query_file
    query_file=$(get_query_file "$ip" "$port")

    if [[ ! -f "$query_file" ]]; then
        echo "Error: Query file $query_file not found for $ip:$port"
        return
    fi

    echo "Using query file: $query_file for $ip:$port"

    local start_time_node=$(date +%s%3N)  # Start time for this node
    local query_count=0

    while IFS= read -r key; do
        if [[ -n "$key" ]]; then
            echo "Querying: \"$key\" on $ip:$port"

            local start_query_time=$(date +%s%3N)
            cargo run --release -- cli "$ip" "$port" query "$key"
            local end_query_time=$(date +%s%3N)

            # Add up individual query times for this node
            local_time_node=$((local_time_node + (end_query_time - start_query_time)))
            ((query_count++))
        fi
    done < "$query_file"

    local end_time_node=$(date +%s%3N)
    local total_time_node=$((end_time_node - start_time_node))

    # Update max global time
    if [[ $total_time_node -gt $global_time ]]; then
        global_time=$total_time_node
    fi

    # Prevent division by zero
    if [[ $total_time_node -eq 0 ]]; then
        total_time_node=1
    fi

    # Calculate per-node read throughput
    local throughput_node=$(echo "scale=2; $query_count / ($total_time_node / 1000)" | bc)
    echo "Node $ip:$port: $throughput_node queries/sec (Total: $query_count keys in $total_time_node ms)"

    echo "$total_time_node" >> node_read_times.txt  # Save each node's time for max computation
}

### Global Timing Initialization
global_time=0
rm -f node_read_times.txt  # Clear previous run data

# Run queries concurrently for all nodes
for key in "${!QUERY_NODES[@]}"; do
    ip=$(echo "$key" | cut -d ':' -f 1)
    port=$(echo "$key" | cut -d ':' -f 2)
    query_data "$ip" "$port" &  # Run in parallel
done

# Wait for all background processes to finish
wait

# Calculate the global max time
if [[ -f node_read_times.txt ]]; then
    global_time=$(sort -nr node_read_times.txt | head -n1)  # Get max time across all nodes
fi

# Prevent division by zero
if [[ $global_time -eq 0 ]]; then
    global_time=1
fi

# Calculate overall read throughput
total_keys=500  # Adjust based on actual query count
global_throughput=$(echo "scale=2; $total_keys / ($global_time / 1000)" | bc)

echo "========================================"
echo "Global Time (Max Node Time): $global_time ms"
echo "Global Read Throughput: $global_throughput queries/sec"
echo "✅ Finished querying data on all VMs."