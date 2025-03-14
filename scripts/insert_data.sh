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

# Function to generate a random Spotify track ID (22 characters)
generate_spotify_link() {
    local chars="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    local link="https://open.spotify.com/track/"
    for _ in {1..22}; do
        link+="${chars:RANDOM%${#chars}:1}"
    done
    echo "$link"
}

# Function to get the correct insert file for the IP and port combination
get_insert_file() {
    local ip=$1
    local port=$2
    local key="${ip}:${port}"
    echo "../data/insert/${IP_PORT_MAP[$key]}"
}

# Read each title from the file and insert it into the Chord-DHT system
insert_data() {
    local ip=$1
    local port=$2
    local insert_file
    insert_file=$(get_insert_file "$ip" "$port")  # Get the correct insert file
    
    if [[ ! -f "$insert_file" ]]; then
        echo "Error: Insert file $insert_file not found for $ip:$port"
        return
    fi

    echo "Using insert file: $insert_file for $ip:$port"

    local start_time_node=$(date +%s%3N)  # Start time for this node
    local insert_count=0

    # Read the file and perform insertions
    while IFS= read -r title; do
        if [[ -n "$title" ]]; then
            spotify_link=$(generate_spotify_link)
            echo "Inserting: \"$title\" -> $spotify_link on $ip:$port"

            # Measure time per insert
            local start_insert_time=$(date +%s%3N)
            cargo run --release -- cli "$ip" "$port" insert "$title" "$spotify_link"
            local end_insert_time=$(date +%s%3N)
            
            # Add up individual insert times for this node
            local_time_node=$((local_time_node + (end_insert_time - start_insert_time)))
            ((insert_count++))
        fi
    done < "$insert_file"

    local end_time_node=$(date +%s%3N)  # End time for this node
    local total_time_node=$((end_time_node - start_time_node))

    # Update max global time
    if [[ $total_time_node -gt $global_time ]]; then
        global_time=$total_time_node
    fi

    # Prevent division by zero
    if [[ $total_time_node -eq 0 ]]; then
        total_time_node=1
    fi

    # Calculate per-node throughput
    local throughput_node=$(echo "scale=2; $insert_count / ($total_time_node / 1000)" | bc)
    echo "Node $ip:$port: $throughput_node inserts/sec (Total: $insert_count keys in $total_time_node ms)"

    echo "$total_time_node" >> node_times.txt  # Save each node's time for max computation
}

### Global Timing Initialization
global_time=0
rm -f node_times.txt  # Clear previous run data

# Loop through the IP_PORT_MAP and run the insertion concurrently
for key in "${!IP_PORT_MAP[@]}"; do
    ip=$(echo "$key" | cut -d ':' -f 1)
    port=$(echo "$key" | cut -d ':' -f 2)
    insert_data "$ip" "$port" &  # Run the insert data function in the background
done

# Wait for all background processes to finish
wait

# Calculate the global max time
if [[ -f node_times.txt ]]; then
    global_time=$(sort -nr node_times.txt | head -n1)  # Get max time across all nodes
fi

# Prevent division by zero
if [[ $global_time -eq 0 ]]; then
    global_time=1
fi

# Calculate overall throughput
total_keys=500  # Adjust based on actual data
global_throughput=$(echo "scale=2; $total_keys / ($global_time / 1000)" | bc)

echo "========================================"
echo "Global Time (Max Node Time): $global_time ms"
echo "Global Throughput: $global_throughput inserts/sec"
echo "✅ Finished inserting data on all VMs."