#!/bin/bash
rm -rf exptwo
sizes=(10240 1048576 104857600)
windows=(100 16 3)
num_objs=(1000000 20000 100)

for i in {0..2}
do
    size=${sizes[i]}
    echo $size
    echo $size >> exptwo
    for j in {1..5}; do
	    rm -rf /mnt/plog/.plog/
	    taskset 0xaaaa ./objectstore_fixnumobj --DERECHO/window_size=${windows[i]} --DERECHO/max_payload_size=$size -- aio ${num_objs[i]} $1 > output
	    cat output | grep -m1 throughput | sed 's/:/ /' | awk '{print $2}' | grep -o '[[:digit:]]\+\.[[:digit:]]\+' >> exptwo
	    sleep 2
	    echo "inner done.."
    done
    echo "done.."
done
