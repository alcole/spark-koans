#!/usr/bin/env python3
"""
Export PySpark Koans to a Jupyter Notebook for testing and verification.

This script reads all koan .js files and creates a notebook with executable cells.
"""

import os
import re
import json
from pathlib import Path
from typing import Dict, List, Optional


def extract_js_string(content: str, field_name: str) -> Optional[str]:
    """Extract a multiline string field from JavaScript object."""
    # Pattern for both backtick strings and regular strings
    pattern = rf'{field_name}:\s*`([^`]*)`'
    match = re.search(pattern, content, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try single-line string
    pattern = rf'{field_name}:\s*["\']([^"\']*)["\']'
    match = re.search(pattern, content)
    if match:
        return match.group(1).strip()

    return None


def extract_koan_data(file_path: Path) -> Dict:
    """Extract koan data from a .js file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Extract fields
    id_match = re.search(r'id:\s*(\d+)', content)
    title = extract_js_string(content, 'title')
    category = extract_js_string(content, 'category')
    setup = extract_js_string(content, 'setup')
    template = extract_js_string(content, 'template')
    solution = extract_js_string(content, 'solution')

    koan_id = int(id_match.group(1)) if id_match else None

    return {
        'id': koan_id,
        'title': title,
        'category': category,
        'setup': setup or '',
        'template': template or '',
        'solution': solution or '',
        'file': file_path.name
    }


def create_koan_cell(koan: Dict, include_solution: bool = True) -> str:
    """Create the code content for a koan cell."""
    lines = []

    # Header comment
    lines.append(f"# Koan {koan['id']}: {koan['title']}")
    lines.append(f"# Category: {koan['category']}")
    lines.append("")

    # Setup code
    if koan['setup']:
        lines.append("# Setup")
        lines.append(koan['setup'])
        lines.append("")

    if include_solution:
        # Solution version - replace template with solution
        lines.append("# Solution")

        # Extract just the solution line(s) and the assertions
        template_lines = koan['template'].split('\n')
        solution_inserted = False

        for line in template_lines:
            # Skip comments and empty lines at the start
            if line.strip().startswith('#') or not line.strip():
                if solution_inserted or not line.strip().startswith('# Create') and not line.strip().startswith('# Write'):
                    lines.append(line)
                continue

            # If this line has ___ and we haven't inserted solution yet
            if '___' in line and not solution_inserted:
                lines.append(koan['solution'])
                solution_inserted = True
            elif 'assert' in line or 'print(' in line or solution_inserted:
                # Include assertions and print statements
                lines.append(line)
    else:
        # Template version - keep blanks for practice
        lines.append("# Exercise (fill in the blanks)")
        lines.append(koan['template'])

    return '\n'.join(lines)


def create_notebook(koans: List[Dict], output_path: Path, include_solutions: bool = True):
    """Create a Jupyter notebook from koans."""

    # Sort koans by ID
    koans_sorted = sorted(koans, key=lambda k: k['id'] if k['id'] else 0)

    # Create notebook structure
    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.9.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

    # Add title cell
    title_cell = {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "# PySpark Koans - Test Notebook\n",
            "\n",
            f"This notebook contains {'solutions for' if include_solutions else ''} all {len(koans_sorted)} koans for testing and verification.\n",
            "\n",
            "**Note**: These koans are designed to work with the browser-based pandas shim. ",
            "To run with real PySpark, you'll need a Spark environment.\n",
            "\n",
            "## Categories:\n",
            "- **Koans 1-30**: PySpark Basics and Operations\n",
            "- **Koans 101-110**: Delta Lake\n",
            "- **Koans 201-210**: Unity Catalog\n",
            "- **Koans 301-310**: Pandas API on Spark"
        ]
    }
    notebook["cells"].append(title_cell)

    # Add initialization cell
    init_cell = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Initialize PySpark (uncomment if running with real Spark)\n",
            "# from pyspark.sql import SparkSession\n",
            "# from pyspark.sql.functions import *\n",
            "# from delta import *\n",
            "#\n",
            "# spark = SparkSession.builder \\\n",
            "#     .appName(\"PySpark Koans\") \\\n",
            "#     .config(\"spark.sql.extensions\", \"io.delta.sql.DeltaSparkSessionExtension\") \\\n",
            "#     .config(\"spark.sql.catalog.spark_catalog\", \"org.apache.spark.sql.delta.catalog.DeltaCatalog\") \\\n",
            "#     .getOrCreate()\n",
            "\n",
            "# For browser-based version, spark is already initialized\n",
            "# This notebook assumes you have PySpark available\n",
            "\n",
            "print(\"✓ Environment ready\")"
        ]
    }
    notebook["cells"].append(init_cell)

    # Group koans by category
    categories = {}
    for koan in koans_sorted:
        cat = koan['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(koan)

    # Add koans grouped by category
    for category, category_koans in categories.items():
        # Category header
        category_cell = {
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"## {category}"]
        }
        notebook["cells"].append(category_cell)

        # Add each koan in the category
        for koan in category_koans:
            koan_cell = {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": create_koan_cell(koan, include_solutions)
            }
            notebook["cells"].append(koan_cell)

    # Write notebook
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

    print(f"✓ Created notebook: {output_path}")
    print(f"  Total koans: {len(koans_sorted)}")
    print(f"  Categories: {len(categories)}")


def main():
    # Find all koan files
    koans_dir = Path(__file__).parent / 'next-app' / 'src' / 'koans'

    if not koans_dir.exists():
        print(f"Error: Koans directory not found: {koans_dir}")
        return

    # Collect all koan files
    koan_files = []
    for pattern in ['**/*koan-*.js']:
        koan_files.extend(koans_dir.glob(pattern))

    # Exclude index.js
    koan_files = [f for f in koan_files if 'index.js' not in f.name]

    print(f"Found {len(koan_files)} koan files")

    # Extract data from each koan
    koans = []
    for koan_file in koan_files:
        try:
            koan_data = extract_koan_data(koan_file)
            if koan_data['id']:
                koans.append(koan_data)
                print(f"  ✓ Koan {koan_data['id']:3d}: {koan_data['title']}")
        except Exception as e:
            print(f"  ✗ Error processing {koan_file.name}: {e}")

    # Create notebooks
    output_dir = Path(__file__).parent

    # Solutions notebook
    create_notebook(
        koans,
        output_dir / 'koans_solutions.ipynb',
        include_solutions=True
    )

    # Practice notebook (with blanks)
    create_notebook(
        koans,
        output_dir / 'koans_practice.ipynb',
        include_solutions=False
    )

    print(f"\n✓ Done! Created 2 notebooks:")
    print(f"  - koans_solutions.ipynb (with answers)")
    print(f"  - koans_practice.ipynb (with blanks)")


if __name__ == '__main__':
    main()
