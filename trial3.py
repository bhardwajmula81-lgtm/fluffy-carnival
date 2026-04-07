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

# Using '=' ensures these only evaluate AFTER export.log is generated
PATH_FROM_EXPORT_LOG  = $(shell cat export.log | grep "netlist" | awk '{print $$NF}')
PRE_NET_VER           = $(shell echo $(PATH_FROM_EXPORT_LOG) | awk -F'/' '{print $$7}')
PRE_REVISION          = $(shell echo $(PATH_FROM_EXPORT_LOG) | awk -F'/' '{print $$9}')

# Maintained colons as requested
CLEAN_SCRIPT          := :clean.sh
PRE_STA_RUN_FILE      := :pre.sh
EXPORT_STA_RUN_FILE   := :export.sh

# The 'all' target only needs the FINAL endpoints. 
# Make handles the prerequisite chain automatically in parallel.
all: run_fm run_vslp export_pre_sta

# ---------------------------------------------------------
# Step 1-3: Linear execution up to Configuration
# ---------------------------------------------------------
wait_for_pass:
	make clean && make compile_opt
	@echo "Waiting for compile_opt.pass file ..."
	@while [ ! -f pass/compile_opt.pass ] || [ ! -f 0__read_floorplan.compile_opt.log ] ; do \
		sleep 10; \
	done
	@echo "compile_opt.pass and 0__read_floorplan.compile_opt.log found"

export_fc: wait_for_pass
	make export

update_config: export_fc
	@echo "Extracted PRE_$(DESIGN)_NET_VER: $(PRE_NET_VER)"
	@echo "Extracted PRE_$(DESIGN)_REVISION: $(PRE_REVISION)"
	@flock $(IMP_PRJCONFIG) -c 'sed -i "s/^PRE_$(DESIGN)_NET_VER.*/PRE_$(DESIGN)_NET_VER                    $(PRE_NET_VER)/" $(IMP_PRJCONFIG) && sed -i "s/^PRE_$(DESIGN)_REVISION.*/PRE_$(DESIGN)_REVISION                    $(PRE_REVISION)/" $(IMP_PRJCONFIG)'

# ---------------------------------------------------------
# Step 4: Parallel Branching (These all wait for update_config)
# ---------------------------------------------------------
fm_upf: update_config
	@echo "Starting UPF FM Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(FM_DIR1) && make clean && make
	@echo "Waiting for any *.final.rpt in reports..."
	@while [ -z "$$(ls $(FM_DIR1)/reports/*.final.rpt 2>/dev/null)" ]; do \
		sleep 5; \
		echo "Still waiting for FM UPF run to be completed..."; \
	done
	@echo "Run is completed"
	@cd $(FM_DIR1) && make export

fm_non_upf: update_config
	@echo "Starting NON-UPF FM Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(FM_DIR2) && make clean && make
	@echo "Waiting for any *.final.rpt in reports..."
	@while [ -z "$$(ls $(FM_DIR2)/reports/*.final.rpt 2>/dev/null)" ]; do \
		sleep 5; \
		echo "Still waiting for FM NON-UPF run to be completed..."; \
	done
	@echo "Run is completed"
	@cd $(FM_DIR2) && make export

run_fm: fm_upf fm_non_upf

run_vslp: update_config
	@echo "Starting VSLP run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(VSLP_DIR) && make clean && make
	@while [ ! -f $(VSLP_DIR)/vslp.done ]; do \
		sleep 5; \
		echo "Still waiting for VSLP run to be completed..."; \
	done
	@echo "Export VSLP run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(VSLP_DIR) && make export

run_pre_sta: update_config
	@echo "Starting PRE-STA Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(PRE_STA_DIR) && ./$(CLEAN_SCRIPT) && ./$(PRE_STA_RUN_FILE)

# ---------------------------------------------------------
# Step 5: Final Exports that depend on parallel tasks
# ---------------------------------------------------------
export_pre_sta: run_pre_sta
	@echo "Exporting PRE-STA Run for : $(PATH_FROM_EXPORT_LOG)"
	@cd $(PRE_STA_DIR) && ./$(EXPORT_STA_RUN_FILE)

.PHONY: all wait_for_pass export_fc update_config run_fm fm_upf fm_non_upf run_vslp run_pre_sta export_pre_sta
default: all
