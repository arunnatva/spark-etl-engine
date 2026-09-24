#!/bin/bash
#
if [[ $# -ne 3 ]]; then
    echo "**************************************"
    echo "**** Usage: scripts/spark_config_gen.sh  <input mapping JSON path>  <output connections JSON path>  <output vars JSON path>"
    echo "**************************************"
    exit 1
fi

python /home/anatva/eth_spark/src/gen_configs.py --mapping "$1" --out-connections "$2" --out-vars "$3"
