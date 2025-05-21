#!/bin/bash
# start_publishers.sh (with debugging)

set -e # Exit immediately if a command exits with a non-zero status.

NUM_INSTANCES_TO_RUN=${1:-2} # Let's default to 2 for this test
PUBLISHER_SCRIPT_PATH="./publisher.py"
VENV_PATH="./mqtt_venv/bin/activate" # YOUR VENV PATH

if [ ! -f "${VENV_PATH}" ]; then
    echo "ERROR: Virtual environment activation script not found at ${VENV_PATH}"
    exit 1
fi

echo "Attempting to activate virtual environment: ${VENV_PATH}"
source "${VENV_PATH}"
echo "Virtual environment source command exit status: $?"

echo "-----------------------------------------------------"
echo "DEBUG INFO AFTER VENV ACTIVATION (from script's perspective):"
echo "Using python3 from: $(which python3)"
echo "Python3 version: $(python3 --version)" # Changed -V to --version for broader compatibility
echo "PYTHONPATH: ${PYTHONPATH}"
echo "PATH: ${PATH}"
echo "-----------------------------------------------------"

# Test Paho import directly and log it for the first instance for debugging
echo "Attempting to import Paho for instance 1 (check publisher_1.log)..."
python3 -c "import paho.mqtt.client; print('Instance 1: Paho-MQTT imported successfully by Python interpreter inside script.')" > "publisher_1.log" 2>&1

echo "Launching ${NUM_INSTANCES_TO_RUN} publisher instances..."

for i in $(seq 1 $NUM_INSTANCES_TO_RUN); do
  echo "Starting publisher instance $i..."
  if [ "$i" -eq 1 ]; then
    # Append the actual publisher script output to the log that already has the import test
    python3 "${PUBLISHER_SCRIPT_PATH}" $i >> "publisher_${i}.log" 2>&1 &
  else
    # For other instances, create/overwrite their log files
    # Let's add an import test for instance 2 as well for comparison
    python3 -c "import paho.mqtt.client; print('Instance ${i}: Paho-MQTT imported successfully by Python interpreter inside script.')" > "publisher_${i}.log" 2>&1
    python3 "${PUBLISHER_SCRIPT_PATH}" $i >> "publisher_${i}.log" 2>&1 &
  fi
done

echo "All ${NUM_INSTANCES_TO_RUN} publisher instances launched in the background."
# ... (rest of the script) ...