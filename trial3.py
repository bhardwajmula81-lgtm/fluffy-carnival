#SHELL = /bin/sh
DESIGN = AUTO_DETECT
WAIT_TIME = 30

# Force Make to run independent targets in parallel
MAKEFLAGS += -j

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
PROJECT_DIR           := $(shell projconf.pl -get PROJECT_DIR)
FM_DIR1               := $(IMPL_DIR)/${PROJECT_NAME}/SOC/${DESIGN}/fm/r2upf
FM_DIR2               := $(IMPL_DIR)/${PROJECT_NAME}/SOC/${DESIGN}/fm/r2n
IMP_PRJCONFIG         := $(IMPL_DIR)/${PROJECT_NAME}/PRJENV/prj.config
VSLP_DIR              := $(IMPL_DIR)/${PROJECT_NAME}/SOC/${DESIGN}/vslp/pre
PRE_STA_DIR           := $(IMPL_DIR)/${PROJECT_NAME}/SOC/${DESIGN}/sta/pre

PATH_FROM_EXPORT_LOG  = $(shell cat export.log | grep "netlist" | awk '{print $$NF}')
PRE_NET_VER           = $(shell echo $(PATH_FROM_EXPORT_LOG) | awk -F'/' '{print $$7}')
PRE_REVISION          = $(shell echo $(PATH_FROM_EXPORT_LOG) | awk -F'/' '{print $$9}')

CLEAN_SCRIPT          := :clean.sh
PRE_STA_RUN_FILE      := :pre.sh
EXPORT_STA_RUN_FILE   := :export.sh

# The 'all' target endpoints
all: run_fm run_vslp export_pre_sta

# ---------------------------------------------------------
# Step 0: Initialize Dashboard Tracking
# ---------------------------------------------------------
setup_tracker:
	@mkdir -p .run_status
	@echo "In progress" > .run_status/synth.stat
	@echo "Waiting" > .run_status/fm_upf.stat
	@echo "Waiting" > .run_status/fm_non_upf.stat
	@echo "Waiting" > .run_status/vslp.stat
	@echo "Waiting" > .run_status/pre_sta.stat
	@echo '#!/bin/bash' > tracker.sh
	@echo 'while true; do' >> tracker.sh
	@echo '  clear' >> tracker.sh
	@echo '  echo "=========================================================================================="' >> tracker.sh
	@echo '  echo "                                PARALLEL RUN STATUS TRACKER                               "' >> tracker.sh
	@echo '  echo "=========================================================================================="' >> tracker.sh
	@echo '  printf "%-16s | %-14s | %-14s | %-14s | %-14s\n" "SYNTHESIS" "FM UPF" "FM NON-UPF" "VSLP" "PRE-STA"' >> tracker.sh
	@echo '  echo "-----------------|----------------|----------------|----------------|----------------"' >> tracker.sh
	@echo '  S1=$$(cat .run_status/synth.stat 2>/dev/null)' >> tracker.sh
	@echo '  F1=$$(cat .run_status/fm_upf.stat 2>/dev/null)' >> tracker.sh
	@echo '  F2=$$(cat .run_status/fm_non_upf.stat 2>/dev/null)' >> tracker.sh
	@echo '  V1=$$(cat .run_status/vslp.stat 2>/dev/null)' >> tracker.sh
	@echo '  P1=$$(cat .run_status/pre_sta.stat 2>/dev/null)' >> tracker.sh
	@echo '  printf "%-16s | %-14s | %-14s | %-14s | %-14s\n" "$$S1" "$$F1" "$$F2" "$$V1" "$$P1"' >> tracker.sh
	@echo '  if [[ "$$S1" == "Completed" && "$$F1" == "Completed" && "$$F2" == "Completed" && "$$V1" == "Completed" && "$$P1" == "Completed" ]]; then' >> tracker.sh
	@echo '    echo -e "\nAll runs completed successfully! Window will close in 10s..."; sleep 10; exit 0' >> tracker.sh
	@echo '  fi' >> tracker.sh
	@echo '  sleep 2' >> tracker.sh
	@echo 'done' >> tracker.sh
	@chmod +x tracker.sh
	@xterm -T "Job Tracker: $(DESIGN)" -geometry 100x12 -e ./tracker.sh 2>/dev/null &

# ---------------------------------------------------------
# Step 1-3: Linear execution up to Configuration
# ---------------------------------------------------------
wait_for_pass: setup_tracker
	make clean && make compile_opt
	@echo "Waiting for compile_opt.pass file ..."
	@while [ ! -f pass/compile_opt.pass ] || [ ! -f 0__read_floorplan.compile_opt.log ] ; do \
		sleep 10; \
	done
	@echo "compile_opt.pass and 0__read_floorplan.compile_opt.log found"
	@echo "Completed" > .run_status/synth.stat

export_fc: wait_for_pass
	make export

update_config: export_fc
	@echo "Extracted PRE_$(DESIGN)_NET_VER: $(PRE_NET_VER)"
	@echo "Extracted PRE_$(DESIGN)_REVISION: $(PRE_REVISION)"
	@flock $(IMP_PRJCONFIG) -c 'sed -i "s/^PRE_$(DESIGN)_NET_VER.*/PRE_$(DESIGN)_NET_VER                    $(PRE_NET_VER)/" $(IMP_PRJCONFIG) && sed -i "s/^PRE_$(DESIGN)_REVISION.*/PRE_$(DESIGN)_REVISION                    $(PRE_REVISION)/" $(IMP_PRJCONFIG)'

# ---------------------------------------------------------
# Step 4: Parallel Branching with Status Updates
# ---------------------------------------------------------
fm_upf: update_config
	@echo "In progress" > .run_status/fm_upf.stat
	@echo "Starting UPF FM Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(FM_DIR1) && make clean && make
	@while [ -z "$$(ls $(FM_DIR1)/reports/*.final.rpt 2>/dev/null)" ]; do \
		sleep 5; \
	done
	@cd $(FM_DIR1) && make export
	@echo "Completed" > .run_status/fm_upf.stat

fm_non_upf: update_config
	@echo "In progress" > .run_status/fm_non_upf.stat
	@echo "Starting NON-UPF FM Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(FM_DIR2) && make clean && make
	@while [ -z "$$(ls $(FM_DIR2)/reports/*.final.rpt 2>/dev/null)" ]; do \
		sleep 5; \
	done
	@cd $(FM_DIR2) && make export
	@echo "Completed" > .run_status/fm_non_upf.stat

run_fm: fm_upf fm_non_upf

run_vslp: update_config
	@echo "In progress" > .run_status/vslp.stat
	@echo "Starting VSLP run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(VSLP_DIR) && make clean && make
	@while [ ! -f $(VSLP_DIR)/vslp.done ]; do \
		sleep 5; \
	done
	@cd $(VSLP_DIR) && make export
	@echo "Completed" > .run_status/vslp.stat

run_pre_sta: update_config
	@echo "In progress" > .run_status/pre_sta.stat
	@echo "Starting PRE-STA Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(PRE_STA_DIR) && ./$(CLEAN_SCRIPT) && ./$(PRE_STA_RUN_FILE)

export_pre_sta: run_pre_sta
	@echo "Exporting PRE-STA Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(PRE_STA_DIR) && ./$(EXPORT_STA_RUN_FILE)
	@echo "Completed" > .run_status/pre_sta.stat

.PHONY: all setup_tracker wait_for_pass export_fc update_config run_fm fm_upf fm_non_upf run_vslp run_pre_sta export_pre_sta
default: all
