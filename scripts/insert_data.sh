#!/bin/bash

# Define the IP addresses and ports
declare -A IP_PORT_MAP
IP_PORT_MAP=(
    ["10.0.24.44:8000"]="insert_00_part.txt"
    ["10.0.24.44:8001"]="insert_01_part.txt"
    ["10.0.24.219:8001"]="insert_10_part.txt"
    ["10.0.24.219:8002"]="insert_11_part.txt"
    ["10.0.24.212:8001"]="insert_20_part.txt"
    ["10.0.24.212:8002"]="insert_21_part.txt"
    ["10.0.24.124:8001"]="insert_30_part.txt"
    ["10.0.24.124:8002"]="insert_31_part.txt"
    ["10.0.24.206:8001"]="insert_40_part.txt"
    ["10.0.24.206:8002"]="insert_41_part.txt"
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
    echo "${IP_PORT_MAP[$key]}"
}

# Read each title from the file and insert it into the Chord-DHT system
insert_data() {
    local ip=$1
    local port=$2
    local insert_file
    insert_file=$(get_insert_file "$ip" "$port")  # Get the correct insert file
    
    echo "Using insert file: $insert_file for $ip:$port"

    # Read the file and perform the insertions
    while IFS= read -r title; do
        if [[ -n "$title" ]]; then
            spotify_link=$(generate_spotify_link)
            echo "Inserting: \"$title\" -> $spotify_link on $ip:$port"
            cargo run cli "$ip" "$port" insert "$title" "$spotify_link"
        fi
    done < "$insert_file"
}

# Loop through the IP_PORT_MAP and run the insertion concurrently
for key in "${!IP_PORT_MAP[@]}"; do
    ip=$(echo "$key" | cut -d ':' -f 1)
    port=$(echo "$key" | cut -d ':' -f 2)
    insert_data "$ip" "$port" &  # Run the insert data function in the background
done

# Wait for all background processes to finish
wait

echo "Finished inserting data on all VMs."
