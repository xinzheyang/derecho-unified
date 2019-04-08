#!/bin/bash
rm -rf exptwo
sizes=(10240 1024000 102400000)
windows=(100 16 3)
num_objs=(100000 10000 1000)

for i in {1..3}
do
    size=${sizes[i]}
    echo $size
    echo $size >> exptwo
    for j in {1..5}; do
	    rm -rf /mnt/plog/.plog/
	    taskset 0xaaaa ./objectstore_fixnumobj --DERECHO/window_size=${windows[i]} --DERECHO/max_payload_size=$size -- aio ${num_objs[i]} $1 > output
	    cat output | grep -m1 throughput | sed 's/:/ /' | awk '{print $2}' | grep -o '[[:digit:]]\+\.[[:digit:]]\+' >> exptwo
	    sleep 2
    done
    echo "done.."
done
