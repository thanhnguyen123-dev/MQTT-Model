echo "Starting 10 MQTT Publisher instances..."

for i in {1..10}
do
   python3 publisher.py "$i" &
done

# to terminate the publishers, run 'pkill -f publisher.py'