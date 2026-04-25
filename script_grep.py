import sys
import os
import re
from pathlib import Path

def parse_area(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        # regex for Number of cells and Total cell area
        cells = re.search(r"Number of cells:\s+(\d+)", content)
        area = re.search(r"Total cell area:\s+([\d.]+)", content)
        
        print(f"--- Area Report ---")
        if cells: print(f"Instance Count: {cells.group(1)}")
        if area: print(f"Total Area: {area.group(1)}")

def parse_utilization(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        # Capturing the AREA(um2) column
        std_cell = re.search(r"std_cell\(\+headbuf\+epbuf\)\s+\d+\s+([\d.]+)", content)
        memory = re.search(r"memory_cell\s+\d+\s+([\d.]+)", content)
        macro = re.search(r"macro_cell\s+\d+\s+([\d.]+)", content)
        
        print(f"--- Utilization Report ---")
        if std_cell: print(f"Std Cell Area: {std_cell.group(1)}")
        if memory: print(f"Memory Area: {memory.group(1)}")
        if macro: print(f"Macro area (Inc. Mem): {macro.group(1)}")

def parse_cell_usage(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        
        lvt_inst, rvt_inst = 0.0, 0.0
        lvt_area, rvt_area = 0.0, 0.0
        
        # Regex to match the table rows and capture Inst % and Area %
        # Example line: | LVT_LLP | 23.838% | 18.240% |
        # We look for the float values
        for line in lines:
            if any(x in line for x in ["LVT", "RVT"]):
                parts = re.findall(r"([\d.]+)%", line)
                if len(parts) >= 2:
                    val_inst, val_area = float(parts[0]), float(parts[1])
                    if "LVT" in line:
                        lvt_inst += val_inst
                        lvt_area += val_area
                    elif "RVT" in line:
                        rvt_inst += val_inst
                        rvt_area += val_area
        
        print(f"--- Cell Usage Summary ---")
        print(f"LVT*/RVT* Inst = {lvt_inst:.2f}%/{rvt_inst:.2f}%")
        print(f"LVT*/RVT* Area = {lvt_area:.2f}%/{rvt_area:.2f}%")

def parse_qor(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        
        def get_r2r_data(section_name):
            # Locate section, then look for the reg->reg column
            section = re.search(f"{section_name}.*?reg->reg", content, re.DOTALL)
            if not section: return None
            
            # Extract the column values below "reg->reg" for WNS, TNS, NUM
            data = re.findall(r"(?:WNS|TNS|NUM)\s+[-]?[\d.]+\s+(-?[\d.]+)", section.group(0))
            return data

        setup = get_r2r_data("Setup violations")
        hold = get_r2r_data("Hold violations")
        
        print(f"--- QoR Report ---")
        if setup: print(f"R2R(Setup) = {'/'.join(setup)}")
        if hold: print(f"R2R(Hold) = {'/'.join(hold)}")

def parse_clock_gating(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        match = re.search(r"Number of Gated registers\s+\d+\s+\(([\d.]+)%\)", content)
        print(f"--- Clock Gating Report ---")
        if match:
            print(f"CGC Ratio = {match.group(1)}%")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3.6 script.py {path_of_run}")
        return

    run_path = Path(sys.argv[1])
    report_dir = run_path / "reports"

    if not report_dir.exists():
        print(f"Error: {report_dir} does not exist.")
        return

    # Map files to their specific parser
    # Using glob to find files regardless of the block name
    files_to_parse = {
        "area": (list(report_dir.glob("area.*.rpt")), parse_area),
        "utilization": (list(report_dir.glob("utilization.*.rpt")), parse_utilization),
        "cell_usage": (list(report_dir.glob("cell_usage.summary.*.rpt")), parse_cell_usage),
        "qor": (list(report_dir.glob("qor.*.rpt")), parse_qor),
        "clock_gating": (list(report_dir.glob("clock_gating_info.mission.rpt")), parse_clock_gating),
    }

    for key, (files, parser) in files_to_parse.items():
        for file in files:
            parser(file)

if __name__ == "__main__":
    main()
