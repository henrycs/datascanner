pid=`ps -ef | grep python | grep app1m.py`
echo $pid
if [ ! -n "$pid" ]; then
    echo "process not found"
else
    echo "process is running"
    exit 0
fi

echo "start process again..."
cd /home/henry/zillionare/omega_scanner_1m
nohup /home/henry/miniconda3/envs/omega/bin/python app1m.py &

