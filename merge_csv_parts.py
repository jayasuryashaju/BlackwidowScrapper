#!/usr/bin/env python3
"""Merge the split Shopify CSV parts back into one file.

Usage:
    python merge_csv_parts.py shopify_products_csv_parts.zip
    python merge_csv_parts.py path/to/folder_with_parts
    python merge_csv_parts.py part1.csv part2.csv ... -o merged.csv

Output defaults to shopify_products_merged.csv. Parts are merged in natural
order (part1, part2, ... part10), with a single header row.

Note: Shopify's importer accepts CSVs up to 15 MB, so the merged file is for
archiving / Google Drive; import the individual parts into Shopify.
"""
import argparse
import csv
import io
import re
import sys
import zipfile
from pathlib import Path


def natural_key(name):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(name))]


def open_parts(inputs):
    """Yield (name, text_stream) for every CSV part found in the inputs."""
    found = []
    for item in inputs:
        p = Path(item)
        if p.is_dir():
            found += [(f.name, f) for f in p.glob("*.csv")]
        elif p.suffix.lower() == ".zip":
            z = zipfile.ZipFile(p)  # kept open until the merge finishes
            for n in z.namelist():
                if n.lower().endswith(".csv") and not n.startswith("__MACOSX"):
                    found.append((Path(n).name, (z, n)))
        elif p.is_file():
            found.append((p.name, p))
        else:
            sys.exit(f"Not found: {item}")
    if not found:
        sys.exit("No CSV parts found")
    found.sort(key=lambda t: natural_key(t[0]))
    for name, src in found:
        if isinstance(src, tuple):
            z, n = src
            yield name, io.TextIOWrapper(z.open(n), encoding="utf-8-sig", newline="")
        else:
            yield name, open(src, encoding="utf-8-sig", newline="")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("inputs", nargs="+", help="a .zip, a folder, or CSV part files")
    ap.add_argument("-o", "--out", default="shopify_products_merged.csv")
    args = ap.parse_args()

    header = None
    rows = products = 0
    with open(args.out, "w", encoding="utf-8-sig", newline="") as out:
        w = csv.writer(out)
        for name, fh in open_parts(args.inputs):
            with fh:
                r = csv.reader(fh)
                h = next(r, None)
                if h is None:
                    continue
                if header is None:
                    header = h
                    w.writerow(header)
                elif h != header:
                    sys.exit(f"{name}: column headers differ from the first part; refusing to merge")
                n = 0
                for row in r:
                    w.writerow(row)
                    n += 1
                    if row and len(row) > 1 and row[1]:  # Title present => first row of a product
                        products += 1
                rows += n
                print(f"  {name}: {n} rows")
    print(f"Merged {rows} rows ({products} products) into {args.out}")


if __name__ == "__main__":
    main()
