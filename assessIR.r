#####
#Run IR assessment scripts
#devtools::document("P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\irTools")

#01. Install UT autoIR package - ignore warnings re: namespace issues - will fix (eventually)
devtools::install_github("ut-ir-tools/irTools")
library(irTools)
#devtools::document("P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\irTools")


##01a. Define function that converts factor columns to numeric values (e.g. ResultMeasureValue and DetectionLimitValue)
## Note - this is now also available via wqTools::facToNum
facToNum=function(x){
  if(class(x)=="factor"){result=as.numeric(levels(x))[x]
  }else{result=x}
  return(result)
}

##02. Retrieve raw data from WQP (narrowresult query can be split apart then bound back together for big data pulls - remove narrowresult from retrieve argument in pullWQP()
?downloadWQP
downloadWQP(outfile_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo2\\01raw_data",
	StartDate="10-01-2010",EndDate="09-30-2016",retry=TRUE,
	retrieve=c("narrowresult","activity","sites","detquantlim")
	)

##03. Read raw data into R, remove duplicates and check for orphans
?readWQPFiles
wqpdat <- readWQPFiles(file_select=FALSE,
            narrowresult_file = "P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\01raw_data\\narrowresult141001-160930.csv",
            sites_file = "P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\01raw_data\\sites141001-160930.csv",
            activity_file = "P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\01raw_data\\activity141001-160930.csv",
            detquantlim_file = "P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\01raw_data\\detquantlim141001-160930.csv",
            orph_check = FALSE)

##04. Auto-validate sites (Note, ignore "attribute variables are assumed to be spatially constant throughout all geometries" warning message - this is a spatial projection/distance measurement warning, but is OK. Warnings to be suppressed later.)
?autoValidateWQPsites
autoValidateWQPsites(
	sites_object=wqpdat$sites,
	master_site_file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\wqp_master_site_file.csv",
	waterbody_type_file = "P:\\WQ\\Integrated Report\\Automation_Development\\elise\\demo\\02site_validation\\waterbody_type_domain_table.csv",
	polygon_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\02site_validation\\polygons",
	outfile_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables"
	)


##05. Site review application
runSiteValApp(
	master_site_file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\wqp_master_site_file.csv",
	polygon_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\02site_validation\\polygons",
	edit_log_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\lookup_tables\\edit_logs",
	reasons_flat_file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\rev_rej_reasons.csv")

#06. Update detection condition / limit name tables
#?updateDetCondLimTables
translation_wb="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\ir_translation_workbook.xlsx"
updateDetCondLimTables(results=wqpdat$merged_results, detquantlim=wqpdat$detquantlim, translation_wb=translation_wb)


#07. Fill masked/censored values in results
#?fillMaskedValues
merged_results_filled=fillMaskedValues(results=wqpdat$merged_results, detquantlim=wqpdat$detquantlim, translation_wb=translation_wb,detsheetname="detLimitTypeTable", unitsheetname="unitConvTable",detstartRow=3, unitstartRow=1, unitstartCol=1, lql_fac=0.5, uql_fac=1)
dim(merged_results_filled)

table(merged_results_filled[merged_results_filled$ActivityIdentifier=="UTAHDWQ_WQX-CUWJPRESDC041615-5913220-0416-Pr-F","CharacteristicName"])[table(merged_results_filled[merged_results_filled$ActivityIdentifier=="UTAHDWQ_WQX-CUWJPRESDC041615-5913220-0416-Pr-F","CharacteristicName"])>0]
table(merged_results_filled[merged_results_filled$ActivityIdentifier=="UTAHDWQ_WQX-CUWJPRESDC041615-5913220-0416-Pr-F" & merged_results_filled$CharacteristicName=="Depth, data-logger (ported)", "IR_Unit"], exclude=NULL)


#08. Update lab/activity & media tables (double check startRow & startCol args)
#?updateLabActMediaTables
updateLabActMediaTables(merged_results_filled, translation_wb=translation_wb, labNameActivityTable_startRow = 2)


#09. Apply detection condition, lab/activity, media, & site review based screen tables (double check startRow args)
#note, may want to add argument(s) for columns to include or exclude - currently merges all columns in the screen table to data, renaming flag and comment columns as defined in args
#Or reduce/reorganize merged result columns to a subset of desired columns following all data prep steps (just to reduce width of data for ease of use and clarity)
#?applyScreenTable

merged_results_filled=applyScreenTable(merged_results_filled,translation_wb=translation_wb,
								sheetname="detConditionTable",startRow=3, flag_col_name="IR_DetCond_FLAG", com_col_name="IR_DetCond_COMMENT", na_dup_err=T)

merged_results_filled=applyScreenTable(merged_results_filled,translation_wb=translation_wb,
								sheetname="labNameActivityTable",startRow=2,flag_col_name="IR_LabAct_FLAG", com_col_name="IR_LabAct_COMMENT", na_dup_err=T)

merged_results_filled=applyScreenTable(merged_results_filled,translation_wb=translation_wb,
								sheetname="activityMediaNameTable",startRow=1, flag_col_name="IR_Media_FLAG", com_col_name="IR_Media_COMMENT", na_dup_err=T)

merged_results_filled=applyScreenTable(merged_results_filled,translation_wb=translation_wb,
								sheetname="masterSiteTable",startRow=1, flag_col_name="IR_Site_FLAG", com_col_name="IR_Site_COMMENT", na_dup_err=T)
head(merged_results_filled)
dim(merged_results_filled)


#10. Subset data (by rows) to desired flag types (keeping IR_Site_FLAG =="REVIEW" for now)
mrf_sub=merged_results_filled[which(
	merged_results_filled$IR_DetCond_FLAG=="ACCEPT" &
	merged_results_filled$IR_LabAct_FLAG=="ACCEPT" &
	merged_results_filled$IR_Media_FLAG=="ACCEPT" &
	(merged_results_filled$IR_Site_FLAG =="ACCEPT")
	),]
dim(mrf_sub)
table(mrf_sub$IR_DetCond_FLAG)
table(mrf_sub$IR_LabAct_FLAG)
table(mrf_sub$IR_Media_FLAG)
table(mrf_sub$IR_Site_FLAG)
table(mrf_sub$CharacteristicName)[table(mrf_sub$CharacteristicName)>0]

#table(droplevels(mrf_sub$CharacteristicName))


#11. Update & apply paramTransTable (generate from subsetted data)
#?updateParamTrans
updateParamTrans(data=mrf_sub, detquantlim=detquantlim,  translation_wb=translation_wb, paramFractionGroup_startCol = 2)

mrf_sub_bk=mrf_sub
#mrf_sub=mrf_sub_bk
#table(mrf_sub_bk$CharacteristicName)

mrf_sub=applyScreenTable(mrf_sub,translation_wb=translation_wb,
									sheetname="paramTransTable",startRow=4,flag_col_name="IR_Parameter_FLAG",com_col_name="IR_Parameter_COMMENT",
									na_dup_err=F)
	#Set na_dup_err=F to proceed with partially completed table. applyScreenTable() exits w/ error if IR_FLAG is not fully filled in when na_err=T (default). If subsetting based on a flag column w/ NAs, these must be dealth with via which() or by explicitly excluding NA rows

table(mrf_sub$IR_Parameter_FLAG)
dim(mrf_sub)
mrf_sub=mrf_sub[mrf_sub$IR_Parameter_FLAG=="ACCEPT" & !is.na(mrf_sub$IR_Parameter_FLAG),]	#IR_FLAG column in parameter table needs to be filled in before applying/subsetting

table(mrf_sub$CharacteristicName)[table(mrf_sub$CharacteristicName)>0]
table(mrf_sub$R3172ParameterName)[table(mrf_sub$R3172ParameterName)>0]

#head(mrf_sub)
dim(mrf_sub)

#12. Update & apply activityCommentTable (Not sure if we're going to fully screen comments yet, but can be updated/applied same as above, recommend subsetting based on parameter screens prior to updating)
#?updateCommentTable


#13. Assign criteria
#?assignCriteria
data_crit=assignCriteria(mrf_sub, crit_wb="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\IR_uses_standards.xlsx",
								  crit_sheetname="R317214DomesticRecAgCriteria_JV", ss_sheetname="R317214SSCriteria_JV", crit_startRow=1, ss_startRow=1, rm_nocrit=FALSE)
any(is.na(data_crit$IR_DetCond_FLAG))
data_crit=data_crit[data_crit$IR_DetCond_FLAG=="ACCEPT",]

table(data_crit$CharacteristicName)[table(data_crit$CharacteristicName)>0]
table(data_crit$R3172ParameterName)[table(data_crit$R3172ParameterName)>0]


#write.csv(file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\example_data.csv",example_data, row.names=F)

#14. Update unit conversion table
#?updateUnitConvTable
updateUnitConvTable(data_crit, translation_wb, sheetname = "unitConvTable")

table(data_crit[data_crit$ActivityIdentifier=="UTAHDWQ_WQX-CUWJPRESDC041615-5913220-0416-Pr-F","R3172ParameterName"])
table(data_crit[data_crit$ActivityIdentifier=="UTAHDWQ_WQX-CUWJPRESDC041615-5913220-0416-Pr-F" & data_crit$CharacteristicName=="Depth, data-logger (ported)", "IR_Unit"], exclude=NULL)


#15. Pre-assessment data prep (still some to do, but operational https://trello.com/c/OkvqshfE/3-final-data-cleanup)

prepped_data=dataPrep(data=data_crit, translation_wb, split_agg_tds=TRUE, crit_wb="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\IR_uses_standards.xlsx",
						unit_sheetname = "unitConvTable", startRow_unit = 1, cf_formulas_sheetname="cf_formulas", startRow_formulas=1)
attach(prepped_data)

table(lake_profiles$R3172ParameterName)[table(lake_profiles$R3172ParameterName)>0]
table(lake_profiles[lake_profiles$ActivityIdentifier=="UTAHDWQ_WQX-CUWJPRESDC041615-5913220-0416-Pr-F" & lake_profiles$CharacteristicName=="Depth, data-logger (ported)", "IR_Unit"], exclude=NULL)

#save(file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\prepped_data.RData", prepped_data)
#save(file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\accepted_data.RData", accepted_data)


#######################
#######################
#Performing assessments

load("P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\prepped_data.rdata")
attach(prepped_data)

#16. Assess conventionals:
#16a. Count exceedances
head(conventionals)
conv_exc=countExceedances(conventionals)
conv_exc[conv_exc$IR_MLID=="UTAHDWQ_WQX-4960740",]

#16b. Assess exceedances (conventionals)
conv_assessed=assessExcCounts(conv_exc, min_n=10, max_exc_pct=10, max_exc_count_id=1)
table(conv_assessed$IR_Cat)
head(conv_assessed[conv_assessed$IR_Cat=="NS",])
conv_assessed[conv_assessed$IR_MLID=="UTAHDWQ_WQX-4929010",]

#17. Assess toxics (non-calculated criteria for now)
#17a. Count exceedances
head(toxics)
toxics_exc=countExceedances(toxics)
toxics_exc[toxics_exc$IR_MLID=="UTAHDWQ_WQX-4929010",]

#17b. Assess exceedances (toxics)
toxics_assessed=assessExcCounts(toxics_exc, min_n=4, max_exc_count=1, max_exc_count_id=0)
table(toxics_assessed$IR_Cat)
head(toxics_assessed[toxics_assessed$IR_Cat=="NS",])
toxics_assessed[toxics_assessed$IR_MLID=="UTAHDWQ_WQX-4929010",]

#18. Assess lake profiles
assessed_profs=assessLakeProfiles(lake_profiles)
lake_profs_assessed=assessed_profs$profile_asmnts_mlid_param
#save(file="F:\\Shiny\\lakeDashBoard\\assessed_profs.rdata",assessed_profs)

#19 Assess e.coli

assess_ecoli = assessEColi(prepped_data$ecoli)
ecoli_assessed = assess_ecoli$rollup2site

#######################
#######################
#Roll Up

site_use_param_assessments=rollUp(data=list(toxics_assessed,conv_assessed,lake_profs_assessed), group_vars=c("ASSESS_ID","AU_NAME", "IR_MLID", "BeneficialUse","R3172ParameterName"), expand_uses=TRUE)

site_use_param_assessments[site_use_param_assessments$R3172ParameterName=="Aluminum" & site_use_param_assessments$AssessCat=="NS",]
toxics[toxics$IR_MLID=="UTAHDWQ_WQX-4929010" & toxics$R3172ParameterName=="Aluminum",]
toxics[toxics$IR_MLID=="UTAHDWQ_WQX-4929100" & toxics$R3172ParameterName=="Aluminum",]


au_use_param_assessments=rollUp(data=c("toxics_assessed","conv_assessed"), group_vars=c("ASSESS_ID","AU_NAME", "BeneficialUse","R3172ParameterName"), expand_uses=FALSE)

head(au_use_param_assessments)
au_use_param_assessments[au_use_param_assessments$ASSESS_ID=="UT16020101-030_00",]

au_use_assessments=rollUp(data=c("toxics_assessed","conv_assessed"), group_vars=c("ASSESS_ID","AU_NAME", "BeneficialUse"), expand_uses=TRUE)
au_use_assessments[au_use_assessments$ASSESS_ID=="UT16020101-030_00",]

au_assessments=rollUp(data=c("toxics_assessed","conv_assessed"), group_vars=c("ASSESS_ID","AU_NAME"), expand_uses=FALSE)
au_assessments[au_assessments$ASSESS_ID=="UT16020101-030_00",]
