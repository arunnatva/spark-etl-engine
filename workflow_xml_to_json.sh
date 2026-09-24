#!/bin/bash
#
if [[ $# -ne 2 ]]; then
    echo "***************************************************************************"
    echo "**** Usage: sh xml_to_json.sh <input XML file path>  <output JSON path>"
    echo "***************************************************************************"
    exit 1
fi
python /home/anatva/eth_spark/src/workflow_xml_to_json.py --input "$1" --output "$2"
#
