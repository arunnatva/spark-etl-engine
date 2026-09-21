#!/usr/bin/env python3

"""
xml_to_json.py

Convert one or more PowerCenter mapping XML exports into the JSON schema
consumed by mapping_engine.py.

Input:
    Directory containing XML files

Output:
    One JSON file per XML file in the output directory
"""

import argparse
import glob
import json
import os
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
        fields.append(
            {
                "name": attr(f, "NAME"),
                "datatype": attr(f, "DATATYPE"),
                "port_type": attr(f, "PORTTYPE"),
                "expression": attr(f, "EXPRESSION"),
                "expression_type": attr(f, "EXPRESSIONTYPE"),
                "default_value": attr(f, "DEFAULTVALUE", ""),
                "precision": attr(f, "PRECISION"),
                "scale": attr(f, "SCALE"),
            }
        )

    table_attrs = {
        attr(ta, "NAME"): attr(ta, "VALUE")
        for ta in tf.findall("TABLEATTRIBUTE")
    }

    return {
        "name": attr(tf, "NAME"),
        "type": attr(tf, "TYPE"),
        "description": attr(tf, "DESCRIPTION", ""),
        "fields": fields,
        "table_attributes": table_attrs,
    }


def parse_instance(inst):
    # Target instances carry session-style overrides as TABLEATTRIBUTE children
    # directly on the INSTANCE element (not on the target-definition
    # TRANSFORMATION). This is where instance-level 'Pre SQL' / 'Post SQL' and
    # 'Target Table Name' live once they are defined at the mapping level.
    table_attrs = {
        attr(ta, "NAME"): attr(ta, "VALUE")
        for ta in inst.findall("TABLEATTRIBUTE")
    }

    result = {
        "name": attr(inst, "NAME"),
        "type": attr(inst, "TYPE"),
        "transformation_name": attr(inst, "TRANSFORMATION_NAME"),
        "transformation_type": attr(inst, "TRANSFORMATION_TYPE"),
    }

    # Keep the full attribute map for completeness, and surface Pre/Post SQL as
    # first-class fields only when they carry a non-empty value (so downstream
    # code can test truthiness without wading through empty strings).
    if table_attrs:
        result["table_attributes"] = table_attrs

    pre_sql = (table_attrs.get("Pre SQL") or "").strip()
    post_sql = (table_attrs.get("Post SQL") or "").strip()
    if pre_sql:
        result["pre_sql"] = pre_sql
    if post_sql:
        result["post_sql"] = post_sql

    return result


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

    if mapping is None:
        raise ValueError("No MAPPING element found in XML.")

    sources = [
        parse_source(s)
        for s in root.findall(".//SOURCE")
        if attr(s, "NAME")
    ]

    targets = [
        parse_target(t)
        for t in root.findall(".//TARGET")
        if attr(t, "NAME")
    ]

    instances = [
        parse_instance(i)
        for i in mapping.findall("INSTANCE")
    ]

    transformation_names = {
        attr(i, "TRANSFORMATION_NAME")
        for i in mapping.findall("INSTANCE")
        if attr(i, "TYPE") == "TRANSFORMATION"
        and attr(i, "TRANSFORMATION_NAME")
    }

    transformations = [
        parse_transformation(tf)
        for tf in root.findall(".//TRANSFORMATION")
        if attr(tf, "NAME") in transformation_names
    ]

    connectors = [
        parse_connector(c)
        for c in mapping.findall("CONNECTOR")
    ]

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

    ap = argparse.ArgumentParser(
        description="Convert Informatica XML mappings to JSON"
    )

    ap.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing XML files"
    )

    ap.add_argument(
        "--output-dir",
        required=True,
        help="Directory where JSON files will be written"
    )

    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    #xml_files = sorted(
    #    glob.glob(
    #        os.path.join(args.input_dir, "m_*.xml")
    #    )
    #)

    xml_files = sorted(
        glob.glob(os.path.join(args.input_dir, "m_*.xml")) +
        glob.glob(os.path.join(args.input_dir, "m_*.XML"))
    )

    if not xml_files:
        print(f"No XML files found in {args.input_dir}")
        return

    print(f"Found {len(xml_files)} XML files.")

    success_count = 0
    failure_count = 0

    for xml_file in xml_files:

        try:

            print(f"\nProcessing: {xml_file}")

            doc = convert(xml_file)

            base_name = os.path.splitext(
                os.path.basename(xml_file)
            )[0]

            json_file = os.path.join(
                args.output_dir,
                f"{base_name}.json"
            )

            with open(json_file, "w") as f:
                json.dump(doc, f, indent=2)

            m = doc["mapping"]

            print(f"Created: {json_file}")

            print(
                f"  sources={len(doc['sources'])} "
                f"targets={len(doc['targets'])} "
                f"transforms={len(m['transformations'])} "
                f"instances={len(m['instances'])} "
                f"connectors={len(m['connectors'])}"
            )

            success_count += 1

        except Exception as e:

            failure_count += 1

            print(
                f"ERROR processing {xml_file}: {str(e)}"
            )

    print("\n===================================")
    print(f"Successfully processed : {success_count}")
    print(f"Failed                 : {failure_count}")
    print("===================================")


if __name__ == "__main__":
    main()
