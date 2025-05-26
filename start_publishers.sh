#!/bin/bash

# Script to start 10 instances of the MQTT publisher

echo "Starting 10 MQTT Publisher instances..."

# Loop from 1 to 10
for i in {1..10}
do
#    echo "Starting publisher instance $i (pub-$i)..."
   # Assuming your publisher script is named publisher.py and is in the current directory
   # The '&' at the end runs the command in the background
   python3 publisher.py "$i" &
   # Optional: Add a small delay if needed, though usually not necessary for just launching
   # sleep 0.1
done

# echo "All 10 publisher instances have been launched in the background."
# echo "They will connect to the broker and wait for commands from the analyser."
# echo "To stop them, you might need to identify their Process IDs (PIDs) or use a command like 'pkill -f \"python3 publisher.py\"'."