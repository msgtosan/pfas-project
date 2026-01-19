#!/usr/bin/env python3
"""Analyze CAMS Excel file using zipfile and XML parsing.

This is a standalone analysis script, not a pytest test.
Run directly: python analyze_cams_file.py
"""

import os
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path


def extract_sheet_data(workbook_xml, sheet_file, sheet_name):
    """Extract data from a sheet XML file."""
    print(f"\n📄 Sheet: {sheet_name}")

    # Parse sheet XML
    try:
        root = ET.fromstring(sheet_file)

        # Namespace
        ns = {'ss': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}

        # Find all rows
        rows = root.findall('.//ss:row', ns)
        print(f"   Total rows: {len(rows)}")

        # Get headers (first row)
        if rows:
            header_cells = rows[0].findall('.//ss:c', ns)
            headers = []
            for cell in header_cells:
                v_elem = cell.find('ss:v', ns)
                if v_elem is None:
                    v_elem = cell.find('ss:t', ns)
                headers.append(v_elem.text if v_elem is not None else "")

            print(f"   Columns: {len(headers)}")
            print(f"   Headers: {headers[:10]}")
            if len(headers) > 10:
                print(f"            ... and {len(headers) - 10} more")

        # Count data rows
        data_rows = len(rows) - 1
        print(f"   Data rows: {data_rows}")

        # Sample first 3 data rows
        if len(rows) > 1:
            print(f"\n   Sample Data (first 2 rows):")
            for row_idx in range(1, min(3, len(rows))):
                row = rows[row_idx]
                cells = row.findall('.//ss:c', ns)
                row_data = []
                for cell in cells[:5]:
                    v_elem = cell.find('ss:v', ns)
                    if v_elem is None:
                        v_elem = cell.find('ss:t', ns)
                    val = v_elem.text if v_elem is not None else ""
                    row_data.append(str(val)[:30] if val else "")
                print(f"      Row {row_idx}: {row_data}")

    except Exception as e:
        print(f"   Error parsing: {e}")


def analyze_cams_file():
    """Main analysis function."""
    # Get file path from environment or use default
    pfas_root = os.getenv("PFAS_ROOT", str(Path.cwd()))
    user_name = os.getenv("PFAS_TEST_USER", "Sanjay")

    cams_file = Path(pfas_root) / "Users" / user_name / "Mutual-Fund" / "CAMS" / "Sanjay_CAMS_CG_FY2024-25_v1.xlsx"

    print("="*70)
    print("CAMS Excel File Analysis")
    print("="*70 + "\n")

    if not cams_file.exists():
        print(f"⚠️  File not found: {cams_file}")
        print(f"\nSet PFAS_ROOT environment variable to your data directory.")
        print(f"Example: PFAS_ROOT=/path/to/data python {__file__}")
        return False

    try:
        with zipfile.ZipFile(cams_file, 'r') as z:
            # Get workbook.xml
            workbook_content = z.read('xl/workbook.xml')
            wb_root = ET.fromstring(workbook_content)

            # Parse sheet names
            ns = {'ss': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            sheets_elem = wb_root.find('.//ss:sheets', ns)

            if sheets_elem is not None:
                sheets = sheets_elem.findall('.//ss:sheet', ns)
                sheet_names = [s.get('name') for s in sheets]
                print(f"📊 Found {len(sheet_names)} sheets:")
                for name in sheet_names:
                    print(f"   - {name}")
            else:
                # Alternative namespace
                ns2 = {}
                sheets = wb_root.findall('.//sheet')
                sheet_names = [s.get('name') for s in sheets]
                print(f"📊 Found {len(sheet_names)} sheets:")
                for name in sheet_names:
                    print(f"   - {name}")

            # Analyze each sheet
            for idx, sheet_name in enumerate(sheet_names, 1):
                sheet_file = z.read(f'xl/worksheets/sheet{idx}.xml')
                extract_sheet_data(workbook_content, sheet_file, sheet_name)

        print("\n" + "="*70)
        print("✅ File analysis complete")
        print("="*70)
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = analyze_cams_file()
    sys.exit(0 if success else 1)
