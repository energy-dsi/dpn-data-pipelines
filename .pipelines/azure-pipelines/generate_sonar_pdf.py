import sys
import os
import glob
import json
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import cm

if len(sys.argv) < 3:
    raise Exception("Usage: python sonar_pdf_styled.py <json_folder> <output_pdf_name>")

JSON_FOLDER = sys.argv[1]
OUTPUT = os.path.join(JSON_FOLDER, sys.argv[2])

print(f"Using JSON folder: {JSON_FOLDER}")
print(f"PDF will be saved at: {OUTPUT}")

json_files = sorted(glob.glob(os.path.join(JSON_FOLDER, "*.json")))
if not json_files:
    raise Exception("❌ No JSON files found in folder: " + JSON_FOLDER)

print("JSON files used for report:")
for f in json_files:
    print(" -", os.path.basename(f))

measures_json = None
extra_fields = {}

for jf in json_files:
    with open(jf, "r") as f:
        data = json.load(f)
        comp = data.get("component")

        if comp:
            if "measures" in comp:
                measures_json = data

            # Capture extra component-level fields
            for k, v in comp.items():
                if k not in ("key", "measures"):
                    extra_fields[k] = v

if not measures_json:
    raise Exception("❌ No file contains 'component.measures' in JSON input")

component = measures_json["component"]
project_key = component.get("key", "Unknown Project")
metric_map = {m["metric"]: m.get("value", "N/A") for m in component["measures"]}

pdf = canvas.Canvas(OUTPUT, pagesize=A4)
width, height = A4
y = height - 100
page = 1

styles = getSampleStyleSheet()
wrap_style = ParagraphStyle(
    'wrap',
    parent=styles['Normal'],
    fontName="Helvetica",
    fontSize=9,
    leading=12,
)

def header():
    pdf.setFillColor(colors.HexColor("#003366"))
    pdf.setFont("Helvetica-Bold", 17)
    pdf.drawString(40, height - 50, f"SonarQube Metrics Report — {project_key}")
    pdf.line(40, height - 60, width - 40, height - 60)

def footer():
    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(colors.grey)
    pdf.drawRightString(width - 40, 25, f"Page {page}")

header()
footer()

def new_page():
    global y, page
    pdf.showPage()
    page += 1
    header()
    footer()
    y = height - 100

def section_title(title):
    global y
    if y < 150:
        new_page()
    pdf.setFont("Helvetica-Bold", 14)
    pdf.setFillColor(colors.HexColor("#003366"))
    pdf.drawString(40, y, title)
    y -= 20

def draw_table(rows):
    global y
    table_data = [["Metric", "Value"]]

    # JSON pretty-printing & wrapping
    for key, val in rows:
        if isinstance(val, (dict, list)):
            pretty = json.dumps(val, indent=2)
            val_para = Paragraph(pretty.replace("\n", "<br/>"), wrap_style)
        else:
            val_para = Paragraph(str(val), wrap_style)
        table_data.append([key, val_para])

    table = Table(table_data, colWidths=[6*cm, 10*cm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#003366")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("GRID", (0,0), (-1,-1), 0.25, colors.black),
        ("BACKGROUND", (0,1), (-1,-1), colors.whitesmoke),
        ("FONTSIZE", (0,0), (-1,-1), 9),
    ]))

    _, table_height = table.wrap(0, 0)

    if y - table_height < 80:
        new_page()

    table.drawOn(pdf, 40, y - table_height)
    y -= table_height + 20

SECTIONS = {
    "Quality Gate": ["alert_status", "quality_gate_details"],
    "Ratings": ["sqale_rating", "reliability_rating", "security_rating", "sqale_debt_ratio"],
    "Issues": [
        "bugs","new_bugs","vulnerabilities","new_vulnerabilities",
        "code_smells","new_code_smells","violations","new_violations","accepted_issues"
    ],
    "Coverage": [
        "coverage","new_coverage","line_coverage","branch_coverage",
        "uncovered_lines","uncovered_conditions"
    ],
    "Duplications": [
        "duplicated_lines_density","new_duplicated_lines_density",
        "duplicated_lines","duplicated_blocks","duplicated_files"
    ],
    "Complexity": ["complexity","cognitive_complexity"],
    "Code Structure": ["ncloc","lines","files","classes","functions","statements"],
    "Security Hotspots": [
        "security_hotspots","new_security_hotspots",
        "security_hotspots_reviewed","new_security_hotspots_reviewed"
    ]
}

for title, keys in SECTIONS.items():
    section_title(title)
    rows = []
    for key in keys:
        if key in metric_map:
            rows.append((key, metric_map[key]))
        elif key in extra_fields:
            rows.append((key, extra_fields[key]))
        else:
            rows.append((key, "N/A"))
    draw_table(rows)

pdf.save()
print(f"PDF generated successfully at: {OUTPUT}")
