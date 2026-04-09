
#!/usr/bin/python
import subprocess
import os
import re
import sys

def read_config(cfg):
    cfg_data = {}

    with open(cfg) as f:
        for line in f:
            if re.search(r"^\s*#", line) or not line.strip():
                continue
            
            if re.search(r"=", line):
                ldata = line.split("=")
                if not ldata or len(ldata) < 2:
                    print(f"\t[Error]: Could not extract key-value pair from line: {line}")
                    continue

                # .strip() cleans spaces around keys and values seamlessly
                cfg_data[ldata[0].strip()] = ldata[1].strip()
    return cfg_data

def run_option(run_type):
    return 1 if run_type == "synth" else 2

# Helper function to determine the PASS_NAME before we generate anything
def get_pass_name(blk, cfg):
    default_pass = cfg.get("DEFAULT_PASS_NAME", "").strip()
    blk_pass     = cfg.get(f"{blk}_PASS_NAME", "").strip()
    return blk_pass if blk_pass else default_pass

def gen_blk_makefile(blk, run_dir, cfg):
    makefile_path = os.path.join(run_dir, f"makefile.{blk}")
    pass_name = get_pass_name(blk, cfg)
    
    # --- Extract TCL variables for this specific block from config ---
    sed_commands = []
    final_vars = {}
    
    # 1. First, find all DEFAULT_user_vars
    def_prefix = "DEFAULT_user_vars("
    for key, val in cfg.items():
        if key.startswith(def_prefix) and key.endswith(")"):
            inner_vars = key[len(def_prefix):-1]
            final_vars[inner_vars] = val
            
    # 2. Next, find all block-specific vars (These will override the defaults)
    blk_prefix = f"BLK_{blk}_user_vars("
    for key, val in cfg.items():
        if key.startswith(blk_prefix) and key.endswith(")"):
            inner_vars = key[len(blk_prefix):-1]
            final_vars[inner_vars] = val  # Overwrites default if it exists
            
    # 3. Generate the sed commands for the final merged variables
    for inner_vars, val in final_vars.items():
        # This regex isolates the quoted value and preserves the semicolon and trailing spaces perfectly
        sed_cmd = f"\t@sed -i 's/\\(^[ \\t]*set[ \\t]*user_vars({inner_vars})[ \\t]*\\)\"[^\"]*\"/\\1\"{val}\"/' $(RUN_DIR)/$(PASS_NAME)-FE/user_design_setup.tcl\n"
        sed_commands.append(sed_cmd)
    
    # -------------------------
    with open(makefile_path, 'w') as f:
        f.write("\nMPNR_CMD=$(COMMON_IMPL_DIR)/common_tcl/mpnr.tcl\n")
        f.write(f"PASS_NAME={pass_name}\n")
        f.write(f"RUN_TYPE={cfg.get('RUN_TYPE')}\n")
        f.write(f"RUN_OPTION={run_option(cfg.get('RUN_TYPE'))}\n")
        f.write(f"RUN_DIR={run_dir}\n")
        f.write(f"\n\nrun_mpnr:\n")
        f.write(f'\t@cd $(RUN_DIR) && echo -e "$(RUN_OPTION)\\n$(PASS_NAME)\\n\\n" | $(MPNR_CMD)\n')
        f.write(f"\tcp -rf /user/s5k2p5sx.fe1/s5k2p5sp/WS/mohit.bhar_S5K2P5SP_ws_22/IMPLEMENTATION/S5K2P5SP/SOC/BLK_CMU/fc/python_script/makefile.flow $(RUN_DIR)/$(PASS_NAME)-FE/\n")
        
        # Write the sed commands right after the folder is populated
        if sed_commands:
            f.write("\t@echo 'Applying user_vars from config (Overrides > Defaults)...'\n")
            for cmd in sed_commands:
                f.write(cmd)
                
        f.write(f"\tcd $(RUN_DIR)/$(PASS_NAME)-FE && make -f makefile.flow\n")
        f.write(f"\n\ndefault:\n")
        f.write(f"\trun_mpnr\n")

    return makefile_path

def command(terminal_cmd: str):
    r = subprocess.run(f"{terminal_cmd}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return r.stdout.decode().strip()

def run_dir_of_blk(blk_name, cfg):
    ws_name = os.getenv("WorkSpace")
    prj_name = command("projconf.pl -get PROJECT_NAME")
    run_path = os.path.join(ws_name, "IMPLEMENTATION", prj_name, "SOC", blk_name, cfg["TOOL_TYPE"])
    return run_path

# Main Execution
cfg = read_config("/user/s5k2p5sx.fe1/s5k2p5sp/WS/mohit.bhar_S5K2P5SP_ws_22/IMPLEMENTATION/S5K2P5SP/SOC/BLK_CMU/fc/python_script_edit/config.cfg")
hpdf_blk = sys.argv[2]
print(f"Target Blocks: {hpdf_blk}")

mf_lst = []

for blk in hpdf_blk.split():
    run_dir = run_dir_of_blk(blk, cfg)

    if not os.path.exists(run_dir):
       print(f"\t[Warning]: Run directory does not exist for {blk}. Skipping...")
       continue
    
    # Check if the PASS_NAME-FE directory already exists
    pass_name = get_pass_name(blk, cfg)
    fe_dir = os.path.join(run_dir, f"{pass_name}-FE")
    
    if os.path.exists(fe_dir):
        print(f"\t[WARNING]: Tag directory '{pass_name}-FE' already exists for block '{blk}'. Skipping this block to prevent overwrite.")
        continue
    
    # If it doesn't exist, safely generate the makefile and add to run list
    mk = gen_blk_makefile(blk, run_dir , cfg)
    mf_lst.append(mk)

# Write the shell script
with open("run.sh", "w") as f:
    if not mf_lst:
        f.write("echo 'No new blocks to run. All requested tags already exist.'\n")
    else:
        for l in mf_lst:
            # The -j flag ensures the parallel flow inside makefile.flow actually triggers!
            f.write(f"make -j -f {l} &\n")
        f.write("wait\n")
        f.write("echo 'All block executions have finished.'\n")
