#!/usr/bin/env python3
"""
xml_to_json.py
Convert a PowerCenter mapping XML export into the JSON schema consumed by
mapping_engine.py (sources / targets / mapping{instances,transformations,connectors}).
"""
import argparse
import json
import xml.etree.ElementTree as ET


def attr(e, name, default=None):
    return e.attrib.get(name, default)


def parse_source(src):
    return {
        "name": attr(src, "NAME"),
        "database_type": attr(src, "DATABASETYPE"),
        "database_name": attr(src, "DBDNAME"),
        "owner_name": attr(src, "OWNERNAME", ""),
        "fields": [
            {
                "name": attr(f, "NAME"),
                "datatype": attr(f, "DATATYPE"),
                "keytype": attr(f, "KEYTYPE"),
                "nullable": attr(f, "NULLABLE"),
                "precision": attr(f, "PRECISION"),
                "scale": attr(f, "SCALE"),
            }
            for f in src.findall("SOURCEFIELD")
        ],
    }


def parse_target(tgt):
    return {
        "name": attr(tgt, "NAME"),
        "database_type": attr(tgt, "DATABASETYPE"),
        "owner_name": attr(tgt, "OWNERNAME", ""),
        "fields": [
            {
                "name": attr(f, "NAME"),
                "datatype": attr(f, "DATATYPE"),
                "keytype": attr(f, "KEYTYPE"),
                "nullable": attr(f, "NULLABLE"),
                "precision": attr(f, "PRECISION"),
                "scale": attr(f, "SCALE"),
            }
            for f in tgt.findall("TARGETFIELD")
        ],
    }


def parse_transformation(tf):
    fields = []
    for f in tf.findall("TRANSFORMFIELD"):
        fields.append({
            "name": attr(f, "NAME"),
            "datatype": attr(f, "DATATYPE"),
            "port_type": attr(f, "PORTTYPE"),
            "expression": attr(f, "EXPRESSION"),
            "expression_type": attr(f, "EXPRESSIONTYPE"),
            "default_value": attr(f, "DEFAULTVALUE", ""),
            "precision": attr(f, "PRECISION"),
            "scale": attr(f, "SCALE"),
        })
    table_attrs = {attr(ta, "NAME"): attr(ta, "VALUE")
                   for ta in tf.findall("TABLEATTRIBUTE")}
    return {
        "name": attr(tf, "NAME"),
        "type": attr(tf, "TYPE"),
        "description": attr(tf, "DESCRIPTION", ""),
        "fields": fields,
        "table_attributes": table_attrs,
    }


def parse_instance(inst):
    return {
        "name": attr(inst, "NAME"),
        "type": attr(inst, "TYPE"),
        "transformation_name": attr(inst, "TRANSFORMATION_NAME"),
        "transformation_type": attr(inst, "TRANSFORMATION_TYPE"),
    }


def parse_connector(c):
    return {
        "from_instance": attr(c, "FROMINSTANCE"),
        "from_instance_type": attr(c, "FROMINSTANCETYPE"),
        "from_field": attr(c, "FROMFIELD"),
        "to_instance": attr(c, "TOINSTANCE"),
        "to_instance_type": attr(c, "TOINSTANCETYPE"),
        "to_field": attr(c, "TOFIELD"),
    }


def convert(xml_path):
    root = ET.parse(xml_path).getroot()
    mapping = root.find(".//MAPPING")

    sources = [parse_source(s) for s in root.findall(".//SOURCE")
               if attr(s, "NAME")]
    targets = [parse_target(t) for t in root.findall(".//TARGET")
               if attr(t, "NAME")]
    transformations = [parse_transformation(t)
                       for t in mapping.findall("TRANSFORMATION")]
    instances = [parse_instance(i) for i in mapping.findall("INSTANCE")]
    connectors = [parse_connector(c) for c in mapping.findall("CONNECTOR")]

    return {
        "sources": sources,
        "targets": targets,
        "mapping": {
            "name": attr(mapping, "NAME"),
            "description": attr(mapping, "DESCRIPTION", ""),
            "instances": instances,
            "transformations": transformations,
            "connectors": connectors,
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    doc = convert(args.input)
    json.dump(doc, open(args.output, "w"), indent=2)
    m = doc["mapping"]
    print(f"Mapping: {m['name']}")
    print(f"  sources={len(doc['sources'])} targets={len(doc['targets'])} "
          f"transforms={len(m['transformations'])} "
          f"instances={len(m['instances'])} connectors={len(m['connectors'])}")


if __name__ == "__main__":
    main()
