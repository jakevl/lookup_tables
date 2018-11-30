#####
#Run IR assessment scripts
#devtools::document("P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\irTools")

#01. Install UT autoIR package - ignore warnings re: namespace issues - will fix (eventually)
devtools::install_github("ut-ir-tools/irTools")#, ref="IR-ML_Name")
library(irTools)
#devtools::document("P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\irTools")

##02. Retrieve raw data from WQP (narrowresult query can be split apart then bound back together for big data pulls - remove narrowresult from retrieve argument in pullWQP()
?downloadWQP
downloadWQP(outfile_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo2\\01raw_data",
	StartDate="10-01-2010",EndDate="09-30-2016",retry=TRUE,
	retrieve=c("narrowresult","activity","sites","detquantlim")
	)


##03. Auto-validate sites (Note, ignore "attribute variables are assumed to be spatially constant throughout all geometries" warning message - this is a spatial projection/distance measurement warning, but is OK. Warnings to be suppressed later.)
?autoValidateWQPsites
autoValidateWQPsites(
	sites_file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\01raw_data\\sites161001-180930.csv",
	#sites_file="P:\\WQ\\Integrated Report\\Automation_Development\\elise\\demo\\01raw_data\\sites101001-180930_EH.csv",
	master_site_file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\wqp_master_site_file.csv",
	polygon_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\02site_validation\\polygons",
	outfile_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables",
	site_type_keep=c(
		"Lake, Reservoir, Impoundment",
		"Stream",
		"Stream: Canal",
		"Stream: Ditch",
		"Spring",
		"River/Stream",
		"Lake",
		"River/Stream Intermittent",
		"River/Stream Perennial",
		"Reservoir",
		"Canal Transport",
		"Canal Drainage",
		"Canal Irrigation")
	)


##04. Site review application
runSiteValApp(
	master_site_file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\wqp_master_site_file.csv",
	polygon_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\02site_validation\\polygons",
	edit_log_path="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\lookup_tables\\edit_logs",
	reasons_flat_file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\rev_rej_reasons.csv")




#03. Read in downloaded files and merge together (w/ example of potential option for selecting files interactively - choose.files() or choose.dir())
setwd("P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo")
#narrowresult=read.csv(choose.files(getwd(), multi=F, caption="Select narrow result file..."))
#activity=read.csv(choose.files(getwd(), multi=F, caption="Select activity file..."))
narrowresult=read.csv("01raw_data\\narrowresult141001-160930.csv")
activity=read.csv("01raw_data\\activity141001-160930.csv")

dim(narrowresult)
merged_results=merge(narrowresult,activity,all.x=T)
dim(merged_results)

#detquantlim=read.csv(choose.files(getwd(), multi=F, caption="Select detection/quantitation limit file..."))
detquantlim=read.csv("01raw_data\\detquantlim141001-160930.csv")


	
#05. Update detection condition / limit name tables
#?updateDetCondLimTables
translation_wb="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\lookup_tables\\ir_translation_workbook.xlsx"
updateDetCondLimTables(results=merged_results, detquantlim=detquantlim, translation_wb=translation_wb)


#06. Fill masked/censored values in results 
#?fillMaskedValues
merged_results_filled=fillMaskedValues(results=merged_results, detquantlim=detquantlim, translation_wb=translation_wb,detsheetname="detLimitTypeTable", unitsheetname="unitConvTable",detstartRow=3, unitstartRow=1, unitstartCol=1, lql_fac=0.5, uql_fac=1)
dim(merged_results_filled)
	
	
#07. Update lab/activity & media tables (double check startRow & startCol args)
#?updateLabActMediaTables
updateLabActMediaTables(merged_results_filled, translation_wb=translation_wb, labNameActivityTable_startRow = 2)


#08. Apply detection condition, lab/activity, media, & site review based screen tables (double check startRow args)
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


#09. Subset data (by rows) to desired flag types (keeping IR_Site_FLAG =="REVIEW" for now)
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

#table(mrf_sub$CharacteristicName)


#10. Update & apply paramTransTable (generate from subsetted data)
#?updateParamTrans
updateParamTrans(data=mrf_sub, detquantlim=detquantlim,  translation_wb=translation_wb, paramFractionGroup_startCol = 2)


mrf_sub_bk=mrf_sub
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
test=mrf_sub[which(mrf_sub$R3172ParameterName=="Boron"),]
table(test$IR_Fraction)
table(test[test$IR_Fraction=="TOTAL","OrganizationFormalName"])

#11. Update & apply activityCommentTable (Not sure if we're going to fully screen comments yet, but can be updated/applied same as above, recommend subsetting based on parameter screens prior to updating)
#?updateCommentTable
	
	
#12. Assign criteria	
#?assignCriteria
data_crit=assignCriteria(mrf_sub, crit_wb="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\04standards\\IR_uses_standards.xlsx",
								  crit_sheetname="R317214DomesticRecAgCriteria_JV", ss_sheetname="R317214SSCriteria_JV", crit_startRow=1, ss_startRow=1)
any(is.na(data_crit$IR_DetCond_FLAG))
data_crit=data_crit[data_crit$IR_DetCond_FLAG=="ACCEPT",]

example_data=data_crit[data_crit$IR_DetCond!="NRV" & !is.na(data_crit$NumericCriterion),c("IR_MLID","ASSESS_ID","AU_NAME","AU_Type","Water_Type","IR_Lat","IR_Long",
				  "R317Descrp","ActivityIdentifier", "ActivityStartDate","R3172ParameterName","BeneficialUse","BEN_CLASS",
				  "ResultMeasureValue","ResultMeasure.MeasureUnitCode","IR_Value","IR_Unit","IR_DetCond","ResultSampleFractionText","IR_Fraction","TargetFraction",
				  "IR_ActivityType","TargetActivityType","AssessmentType","CriterionLabel","CriterionType","DailyAggFun","AsmntAggPeriod","AsmntAggPeriodUnit","AsmntAggFun","NumericCriterion","CriterionUnits",
				  "IR_LowerLimitValue","IR_LowerLimitUnit","IR_UpperLimitValue","IR_UpperLimitUnit","ss_R317Descrp","SSC_StartMon","SSC_EndMon","SSC_MLID","IR_Parameter_FLAG","IR_DetCond_FLAG","IR_LabAct_FLAG","IR_Media_FLAG","IR_Site_FLAG")]			  
head(example_data)

#write.csv(file="P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\example_data.csv",example_data, row.names=F)

#13. Update unit conversion table
#?updateUnitConvTable
updateUnitConvTable(data_crit, translation_wb, sheetname = "unitConvTable")


#14. Pre-assessment data prep (still some to do, but operational https://trello.com/c/OkvqshfE/3-final-data-cleanup)
rm(list=ls(all=TRUE))
load("P:\\WQ\\Integrated Report\\Automation_Development\\R_package\\demo\\ready_for_prep.RData")
prepped_data=dataPrep(data_crit, translation_wb, unit_sheetname = "unitConvTable", startRow = 1)
attach(prepped_data)
levels(conventionals$AsmntAggFun)=append(levels(conventionals$AsmntAggFun),"mean")
conventionals=within(conventionals,{AsmntAggFun[AsmntAggFun=="Average"]="mean"}) #note - this was initially "Average" in the table, updated to 'mean'

#16. Assess conventionals:
#16a. Count exceedances
head(conventionals)
conventionals$CriterionType[is.na(conventionals$CriterionType)]="max" #Note this was missing for some standards in the table when I built ready_for_prep.RData. Error built into countExceedances to check for NAs in this column.
conv_exc=countExceedances(conventionals)
conv_exc[conv_exc$IR_MLID=="UTAHDWQ_WQX-4960740",]

#16b. Assess exceedances (conventionals)
conv_assessed=assessExcCounts(conv_exc, min_n=10, max_exc_pct=10, max_exc_count_id=1)
table(conv_assessed$IR_Cat)
head(conv_assessed[conv_assessed$IR_Cat=="NS",])
conv_assessed[conv_assessed$IR_MLID=="UTAHDWQ_WQX-4960740",]
conv_assessed[conv_assessed$IR_MLID=="UTAHDWQ_WQX-5994740",]

#17. Assess toxics (non-calculated criteria for now)
#17a. Count exceedances
head(toxics)
toxics_exc=countExceedances(toxics)
head(toxics_exc)

#17b. Assess exceedances (toxics)
toxics_assessed=assessExcCounts(toxics_exc, min_n=4, max_exc_count=1, max_exc_count_id=0)
table(toxics_assessed$IR_Cat)
head(toxics_assessed[toxics_assessed$IR_Cat=="NS",])










	


	
	
	
	