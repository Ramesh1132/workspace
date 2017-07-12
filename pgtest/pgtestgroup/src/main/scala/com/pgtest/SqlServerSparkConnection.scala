package com.pgtest
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object SqlServerSparkConnection {
  
    val conf = new SparkConf().setMaster("local[4]").setAppName("my app")
    val sc = new SparkContext(conf);
 val sparksql = new SQLContext(sc)
 
 
  def main(args : Array[String]){
    
    val dimedwsource = sparksql.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/DimEDWSource.csv");
 
 dimedwsource.registerTempTable("dimedwsource")
 
 
 val denorm_as_with_demo_columnstore_as = sparksql.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/denorm_as_with_demo_columnstore_as.csv");
 
 denorm_as_with_demo_columnstore_as.registerTempTable("denorm_as_with_demo_columnstore_as")
 
 
 val DimCustomItemGroup = sparksql.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/DimCustomItemGroup.csv");
 
 DimCustomItemGroup.registerTempTable("DimCustomItemGroup")
 
 val FactSurveyResponsePXDemographic = sparksql.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/FactSurveyResponsePXDemographic.csv");
 
 FactSurveyResponsePXDemographic.registerTempTable("FactSurveyResponsePXDemographic")
 
 
 val DimSurveyItem = sparksql.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/DimSurveyItem.csv");
 
 DimSurveyItem.registerTempTable("DimSurveyItem")

 val lkpSurveyItemCAHPSPhoneCalibration = sparksql.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/lkpSurveyItemCAHPSPhoneCalibration.csv");
 
 lkpSurveyItemCAHPSPhoneCalibration.registerTempTable("lkpSurveyItemCAHPSPhoneCalibration")
 
 // val Service_table_as = sparksql.sql("select * from	denorm_as_with_demo_columnstore_as px left outer join DimCustomItemGroup ig on ig.SurveyItemKey=px.SurveyItemKey LEFT OUTER JOIN lkpSurveyItemCAHPSPhoneCalibration calibr ON px.SurveyItemKey = calibr.SurveyItemKey AND px.PatientDischargeDateKey BETWEEN calibr.PatientDischargeStartDateKey AND calibr.PatientDischargeEndDateKey WHERE	px.SurveyItemAboutYouFlag = 0 AND px.SurveyItemScreenerFlag = 0 AND IgnoredSurveyItemResponseValueFlag = 0 AND px.SurveyItemCAHPSFlag = 1 AND px.SurveyItemRawFlag = 1")
			
		val Service_table_as =sparksql.sql("select * from	denorm_as_with_demo_columnstore_as px left outer join DimCustomItemGroup ig on ig.SurveyItemKey=px.SurveyItemKey LEFT OUTER JOIN lkpSurveyItemCAHPSPhoneCalibration calibr ON px.SurveyItemKey = calibr.SurveyItemKey AND px.PatientDischargeDateKey BETWEEN calibr.PatientDischargeStartDateKey AND calibr.PatientDischargeEndDateKey WHERE px.SurveyItemAboutYouFlag = 0 AND px.SurveyItemScreenerFlag = 0 AND IgnoredSurveyItemResponseValueFlag = 0 AND px.SurveyItemRawFlag = 1")
		
		Service_table_as.registerTempTable("Service_table_as")
		//sparksql.sql("describe Service_table_as ").collect().foreach(println)
		//sparksql.cacheTable("Service_table_as")
		
		//val GroupBy = sparksql.sql("select topclientid,topclientname, EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value union select topclientid,topclientname , EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName, SurveyItemReportName,if(SurveyItemID like '%[a-z,0-9]%',NULL,NULL), demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid, topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value")
		//val RollUp = sparksql.sql(" select topclientid,topclientname , EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName, SurveyItemReportName,if(SurveyItemID like '%[a-z,0-9]%',NULL,NULL), demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid, topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value")
		//val RollUp = 
		   //val GroupBy = sparksql.sql("select topclientid,topclientname, EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value,topclientid,ClientID,ClientFacilityID, demo_one_label, demo_one_value, SurveySectionID, SurveyItemID")
		//val CAHPS_only = sparksql.sql("select * from Service_table_as LEFT OUTER JOIN lkpSurveyItemCAHPSPhoneCalibration calibr ON Service_table_as.surveyitemkey = calibr.surveyitemkey AND Service_table_as.PatientDischargeDateKey BETWEEN calibr.PatientDischargeStartDateKey AND calibr.PatientDischargeEndDateKey ")
 
//val a =  sparksql.sql("describe Service_table_as")
    val GroupBy = sparksql.sql("select topclientid,topclientname,ClientID,ClientFacilityID,EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID  union select topclientid,topclientname,ClientID,if(ClientFacilityID like '%[a-z,0-9]%',NULL,NULL), EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID ")
//CAHPS_only.collect().foreach(println)
//ISNULL(px.InvalidResponseFlag, 0) = 0 AND       surveymodename

//val a = denorm_as_with_demo_columnstore_as. join(DimCustomItemGroup,denorm_as_with_demo_columnstore_as("SurveyItemKey")===DimCustomItemGroup("SurveyItemKey"),"left_outer").join(lkpSurveyItemCAHPSPhoneCalibration,denorm_as_with_demo_columnstore_as("SurveyItemKey")===lkpSurveyItemCAHPSPhoneCalibration("SurveyItemKey"),"left_outer")AND denorm_as_with_demo_columnstore_as.PatientDischargeDateKey BETWEEN lkpSurveyItemCAHPSPhoneCalibration.PatientDischargeStartDateKey AND lkpSurveyItemCAHPSPhoneCalibration.PatientDischargeEndDateKey 

GroupBy.collect.foreach(println)
   
   
  }
}