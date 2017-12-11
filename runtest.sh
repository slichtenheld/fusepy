#!/bin/bash

NUMDATASERVERS=7 # must be less than 9, otherwise conflict with metaserver

rm datastorage*

array=()

for (( i=1; i<=$NUMDATASERVERS; i++ ))
do
    array+=($i$i$i$i)
done

# echo ${array[@]}

echo "python -u ./metaserver.py 9999 > META &"
python -u ./metaserver.py 9999 > META &

for (( i=1; i<=$NUMDATASERVERS; i++ ))
do
    echo "python -u ./dataserver.py $((i - 1)) ${array[@]} > DATA$i &"
    python -u ./dataserver.py $((i - 1)) ${array[@]} > DATA$i &
    #sleep 0.1
done

sleep 2

echo "python -u ./distributedFS.py fusemount/ ${array[@]} > DISTRIBUTEDFS &"
python -u ./distributedFS.py fusemount/ 9999 ${array[@]} > DISTRIBUTEDFS &

sleep 1

./testcases.sh > TEST

sleep 0.2

NUMPROCESSES=$((NUMDATASERVERS + 2))
echo killing $NUMPROCESSES processes

for (( i=1; i<=$NUMPROCESSES; i++ ))
do
    kill %$i
done


