#!/bin/bash
# A skript for measuring execution time of dODo

start=`date +%s`
filename='data/flight_1k.csv'
header=true
seedPort=7877
nodes=4

java -jar target/scala-2.12/dodo-assembly-0.0.1.jar --input-file=$filename --has-header=$header --port=$seedPort&
for ((i=1;i<$nodes;i++));
do
	port=$(($seedPort+$i))
	java -jar target/scala-2.12/dodo-assembly-0.0.1.jar --input-file=$filename --has-header=$header --port=$port --seed-port=$seedPort&
done
wait
end=`date +%s`
echo Execution time was `expr $end - $start` seconds.