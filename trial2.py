HPDF_BLK := $(shell projconf.pl -get HPDF_BLK)
RUN_DIR  := /user/s5k2p5sx.fe1/s5k2p5sp/WS/mohit.bhar_S5K2P5SP_ws_22/IMPLEMENTATION/S5K2P5SP/SOC/BLK_CMU/fc/python_script_edit
RUN_PY   := $(RUN_DIR)/run.py
CONFIG   := $(RUN_DIR)/config.cfg

BLK_ARG  := $(filter-out all,$(MAKECMDGOALS))

# 1. The "Dummy" rule to catch the ISP names
$(BLK_ARG): all
	@:

# 2. Define 'all' FIRST so it is the primary action
all:
	@echo "$(TARGET_LIST)"
	/user/apsoc/Python-3.6.4/bin/python3.6 $(RUN_PY) $(CONFIG) '$(TARGET_LIST)'
	sh run.sh

# 3. Logic to decide the list
ifeq ($(strip $(BLK_ARG)),)
    TARGET_LIST := $(HPDF_BLK)
else
    TARGET_LIST := $(BLK_ARG)
endif



.PHONY: all $(HPDF_BLK)
.DEFAULT_GOAL := all




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

                cfg_data[ldata[0].strip()] = ldata[1].strip()
    return cfg_data

def run_option(run_type):
    return 1 if run_type == "synth" else 2

def gen_blk_makefile(blk, run_dir, cfg):
    makefile_path = os.path.join(run_dir, f"makefile.{blk}")
    default_pass = cfg.get("DEFAULT_PASS_NAME", "").strip()
    blk_pass     = cfg.get(f"{blk}_PASS_NAME", "").strip()
    pass_name = blk_pass if blk_pass else default_pass
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
        f.write(f"\tcd $(RUN_DIR)/$(PASS_NAME)-FE && make -f makefile.flow")
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

cfg = read_config("/user/s5k2p5sx.fe1/s5k2p5sp/WS/mohit.bhar_S5K2P5SP_ws_22/IMPLEMENTATION/S5K2P5SP/SOC/BLK_CMU/fc/python_script_edit/config.cfg")
hpdf_blk = sys.argv[2]
print(hpdf_blk)

mf_lst = []

for blk in hpdf_blk.split():
    run_dir = run_dir_of_blk(blk, cfg)

    if not os.path.exists(run_dir):
       continue
    
    mk = gen_blk_makefile(blk, run_dir , cfg)
    mf_lst.append(mk)

with open("run.sh", "w") as f:
    for l in mf_lst:
        f.write(f"make -f {l} &\n")






#####config
TOOL_TYPE= fc
RUN_TYPE= synth
DEFAULT_PASS_NAME=expt-03
BLK_ISP_PASS_NAME=Trial-01
BLK_CMU_PASS_NAME=Trial-02
BLK_SENSOR_PASS_NAME=Trial-05



  #SHELL = /bin/sh
  DESIGN = AUTO_DETECT
  WAIT_TIME = 30


ifeq ($(DESIGN),AUTO_DETECT)
   DESIGN     := $(shell /bin/pwd | sed -e "s/.*\/SOC\///" | sed -e "s/\/.*//")
endif

 HPDF_BLK              := $(shell projconf.pl -get HPDF_BLK)
 TEST_BLK              := $(shell projconf.pl -get TEST_BLK)
 MODEM_HPDF_BLK_SYN    := $(shell projconf.pl -get MODEM_HPDF_BLK_SYN)
 BIG_BLK               := $(shell projconf.pl -get BIG_BLK)
 HPDF_IP               := $(shell projconf.pl -get HPDF_IP)
 TOP_DESIGN            := $(shell projconf.pl -get TOP_DESIGN)
 PROJECT_NAME          := $(shell projconf.pl -get PROJECT_NAME)
 PROJECT_NICKNAME      := $(shell projconf.pl -get PROJECT_NICKNAME)
 NDM_DIR               := $(shell projconf.pl -get NDM_DIR)
 PROJECT_DIR 	       := $(shell projconf.pl -get PROJECT_DIR)
 FM_DIR1 	       := $(IMPL_DIR)/${PROJECT_NAME}/SOC/${DESIGN}/fm/r2upf
 FM_DIR2 	       := $(IMPL_DIR)/${PROJECT_NAME}/SOC/${DESIGN}/fm/r2n
 IMP_PRJCONFIG		:= $(IMPL_DIR)/${PROJECT_NAME}/PRJENV/prj.config
 VSLP_DIR	       := $(IMPL_DIR)/${PROJECT_NAME}/SOC/${DESIGN}/vslp/pre



# Default/first goal
  all: wait_for_pass export_fc subs update_config run_fm export_fm run_vslp export_vslp
  wait_for_pass:
	make compile_opt
	@echo "Waiting for compile_opt.pass file ..."
	@while [ ! -f pass/compile_opt.pass ] || [ ! -f 0__read_floorplan.compile_opt.log ] ; do \
	sleep 10; \
	done
	@echo "compile_opt.pass and 0__read_floorplan.compile_opt.log found"

 export_fc: 
	make export

 subs: export_fc
	PATH_FROM_LOG := $(shell cat export.log | grep "netlist" | awk '{print $$NF}') 
	PRE_NET_VER := $(shell echo $(PATH_FROM_LOG) | awk -F'/' '{print $$7}') 
	PRE_REVISION := $(shell echo $(PATH_FROM_LOG) | awk -F'/' '{print $$9}') 

 update_config: subs
	@echo "Extracted PRE_$(DESIGN)_NET_VER: $(PRE_NET_VER)"
	@echo "Extracted PRE_$(DESIGN)_REVISION: $(PRE_REVISION)"
	sed -i "s/^PRE_$(DESIGN)_NET_VER.*/PRE_$(DESIGN)_NET_VER                    $(PRE_NET_VER)/" $(IMPL_DIR)/${PROJECT_NAME}/PRJENV/prj.config
	sed -i "s/^PRE_$(DESIGN)_REVISION.*/PRE_$(DESIGN)_REVISION                    $(PRE_REVISION)/" $(IMPL_DIR)/${PROJECT_NAME}/PRJENV/prj.config
 run_fm: update_config
	@echo "Starting UPF and NONUPF FM Runs for : $(PATH_FROM_LOG) "
	#source $(PROJECT_DIR)/PRJENV/prj.cshrc
	cd $(FM_DIR1) && make
	cd $(FM_DIR2) && make

 export_fm: run_fm
	@echo "Export UPF and NONUPF FM Runs for : $(PATH_FROM_LOG) "
	cd $(FM_DIR1) && make export
	cd $(FM_DIR2) && make export

 run_vslp: update_config
	@echo "Starting VSLP run for : $(PATH_FROM_LOG)"
	cd $(VSLP_DIR) && make

 export_vslp: run_vslp
	@echo "Export VSLP run for : $(PATH_FROM_LOG)"
	cd $(VSLP_DIR) && make export

 .PHONY: all wait_for_pass export_fc subs update_config run_fm export_fm run_vslp export_vslp
  default : all
