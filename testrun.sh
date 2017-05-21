#! /bin/bash

# Quick and dirty scipt for testing a cluster of 2 api nodes and 4 worker
# nodes. Will generate events for 5 minutses. Note that the default
# config (etc/cluster.json) defines an hour to last for 60 seconds.
#
# Query with curl in another terminal to check progress:
#  > curl "http://localhost:8000/analytics?timestamp=`date +%s`"
#
# Get analytics per worker:
#  > curl "http://localhost:8000/analytics/perworker?timestamp=`date +%s`"
#
# Get analytics for all hours until now:
# > curl "http://localhost:8000/analytics/allslices"
#

sh start.sh

start=`date +%s`
end=$[$start + 300]
now=$start

echo "GO!"
while [ $now -lt $end ]; do
    curl -XPOST "http://localhost:8000/analytics?user=$RANDOM&click&timestamp=$[$now - $RANDOM / 300 ]"
    curl -XPOST "http://localhost:8001/analytics?user=$RANDOM&impression&timestamp=$[$now - $RANDOM / 300 ]"
    now=`date +%s`
    echo -n "."
done
echo "Done"

echo "Cluster has been left running."
