import sys
import os
import re
import glob

def parse_area(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        cells = re.search(r"Number of cells:\s+(\d+)", content)
        area = re.search(r"Total cell area:\s+([\d.]+)", content)
        
        if cells: print(f"Instance Count : {cells.group(1)}")
        if area: print(f"Total Area : {area.group(1)}")

def parse_utilization(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        # Capturing the 2nd column (AREA(um2))
        std_cell = re.search(r"^\s*std_cell\(\+headbuf\+epbuf\)\s+\d+\s+([\d.]+)", content, re.MULTILINE)
        memory = re.search(r"^\s*memory_cell\s+\d+\s+([\d.]+)", content, re.MULTILINE)
        macro = re.search(r"^\s*macro_cell\s+\d+\s+([\d.]+)", content, re.MULTILINE)
        
        if std_cell: print(f"Std Cell Area : {std_cell.group(1)}")
        if memory: print(f"Memory Area : {memory.group(1)}")
        if macro: print(f"Macro Area : {macro.group(1)}")

def parse_cell_usage(file_path):
    with open(file_path, 'r') as f:
        lvt_inst, rvt_inst = 0.0, 0.0
        lvt_area, rvt_area = 0.0, 0.0
        
        lvt_keys = ["LVT", "LVT_LLP", "LVT_L30L34"]
        rvt_keys = ["RVT", "RVT_LLP", "RVT_L30L34"]
        
        # Flag to only parse "1-1. For all Cells" section
        in_all_cells = False

        for line in f:
            if "1-1. For all Cells" in line:
                in_all_cells = True
            elif "1-2." in line:
                break # Stop reading after the first section
            
            if in_all_cells and "|" in line:
                cols = [c.strip() for c in line.split('|')]
                # Ensure the line has enough columns
                if len(cols) >= 6:
                    vth_type = cols[1]
                    try:
                        inst_pct = float(cols[3].replace('%', ''))
                        area_pct = float(cols[5].replace('%', ''))
                        
                        if vth_type in lvt_keys:
                            lvt_inst += inst_pct
                            lvt_area += area_pct
                        elif vth_type in rvt_keys:
                            rvt_inst += inst_pct
                            rvt_area += area_pct
                    except ValueError:
                        pass # Skip header/footer rows where float conversion fails
        
        print(f"LVT*/RVT* Inst = {lvt_inst:.2f}%/{rvt_inst:.2f}%")
        print(f"LVT*/RVT* Area = {lvt_area:.2f}%/{rvt_area:.2f}%")

def parse_qor(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        
        def get_r2r_data(section_name):
            # Extract the specific section block
            section = re.search(f"{section_name}.*?(?=Report :|$)", content, re.DOTALL)
            if not section: return None
            
            sec_text = section.group(0)
            
            # The 'reg->reg' values are typically the 2nd value after the label (Total is 1st)
            wns = re.search(r"WNS\s+([-\d.]+)\s+([-\d.]+)", sec_text)
            tns = re.search(r"TNS\s+([-\d.]+)\s+([-\d.]+)", sec_text)
            num = re.search(r"NUM\s+([-\d.]+)\s+([-\d.]+)", sec_text)
            
            if wns and tns and num:
                return f"{wns.group(2)}/{tns.group(2)}/{num.group(2)}"
            return None

        setup = get_r2r_data("Setup violations")
        hold = get_r2r_data("Hold violations")
        
        if setup: print(f"R2R (Setup) : {setup}")
        if hold: print(f"R2R (Hold) : {hold}")

def parse_clock_gating(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        # Added '\|' to correctly pass the table formatting
        match = re.search(r"Number of Gated registers\s+\|\s+\d+\s+\(([\d.]+)%\)", content)
        if match:
            print(f"CGC Ratio : {match.group(1)}%")


def parse_multibit(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        match = re.search(r"Flip-flop cells banking ratio \(\(C\)/\((?:A\+C)\)\):\s+([\d.]+)%", content)
        if match:
            print(f"MBIT Ratio : {match.group(1)}%")

def parse_congestion(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        
        both = re.search(r"Both Dirs\s+\|.*?\(.*?([\d.]+)%\)", content)
        h_route = re.search(r"H routing\s+\|.*?\(.*?([\d.]+)%\)", content)
        v_route = re.search(r"V routing\s+\|.*?\(.*?([\d.]+)%\)", content)
        
        if both and h_route and v_route:
            # Outputting as Both/V/H Dir as requested
            print(f"Congestion (Both/V/H Dir) : {both.group(1)}%/{v_route.group(1)}%/{h_route.group(1)}%")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3.6 script.py {path_of_run}")
        return

    # Normalize paths
    run_path = os.path.abspath(sys.argv[1])
    report_dir = os.path.join(run_path, "reports")

    if not os.path.exists(report_dir):
        print(f"Error: Directory {report_dir} does not exist.")
        return
    
    # Mapping report types to file patterns and their corresponding parser
    parsers = [
        ("area.*.rpt", parse_area),
        ("utilization.*.rpt", parse_utilization),
        ("cell_usage.summary.*.rpt", parse_cell_usage),
        ("clock_gating_info.mission.rpt", parse_clock_gating),
        ("qor.*.rpt", parse_qor),
        ("multibit_banking_ratio.*.rpt", parse_multibit),
        ("congestion.*.rpt", parse_congestion) # Adjust pattern if filename differs slightly
    ]

    for pattern, parser_func in parsers:
        search_pattern = os.path.join(report_dir, pattern)
        matching_files = glob.glob(search_pattern)
        
        for file in matching_files:
            try:
                parser_func(file)
            except Exception as e:
                print(f"Error parsing {file}: {e}")

if __name__ == "__main__":
    main()
