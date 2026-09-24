#!/bin/bash
#
if [[ $# -ne 2 ]]; then
    echo "***************************************************************************"
    echo "**** Usage: sh xml_to_json.sh <input Mapping XMLs directory>  <output Mapping JSONs directory>"
    echo "***************************************************************************"
    exit 1
fi
python /home/anatva/eth_spark/src/mapping_xml_to_json.py --input-dir "$1" --output-dir "$2"
#
