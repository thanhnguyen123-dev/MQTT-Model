NUM_INSTANCES_TO_RUN=${1:-3}
PUBLISHER_SCRIPT_PATH="./publisher.py" 
VENV_PATH="./mqtt_venv/bin/activate"

echo "Activating virtual environment: ${VENV_PATH}"
source "${VENV_PATH}"

echo "Launching ${NUM_INSTANCES_TO_RUN} publisher instances..."

for i in $(seq 1 $NUM_INSTANCES_TO_RUN); do
  echo "Starting publisher instance $i..."
  python3 "${PUBLISHER_SCRIPT_PATH}" $i > "publisher_${i}.log" 2>&1 &
done

echo "All ${NUM_INSTANCES_TO_RUN} publisher instances launched in the background."
echo "To see their PIDs: pgrep -f publisher.py"
echo "To stop them later, you can use: pkill -f publisher.py"