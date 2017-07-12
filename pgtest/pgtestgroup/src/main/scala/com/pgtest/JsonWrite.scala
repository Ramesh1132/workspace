package com.pgtest
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
object JsonWrite {
  
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
 
 
			
		val Service_table_as =sparksql.sql("select * from	denorm_as_with_demo_columnstore_as px left outer join DimCustomItemGroup ig on ig.SurveyItemKey=px.SurveyItemKey LEFT OUTER JOIN lkpSurveyItemCAHPSPhoneCalibration calibr ON px.SurveyItemKey = calibr.SurveyItemKey AND px.PatientDischargeDateKey BETWEEN calibr.PatientDischargeStartDateKey AND calibr.PatientDischargeEndDateKey WHERE px.SurveyItemAboutYouFlag = 0 AND px.SurveyItemScreenerFlag = 0 AND IgnoredSurveyItemResponseValueFlag = 0 AND px.SurveyItemRawFlag = 1")
		
		Service_table_as.registerTempTable("Service_table_as")
		
// sparksql.cacheTable("Service_table_as")

   val GroupBy = sparksql.sql("select topclientid,topclientname,ClientID,ClientFacilityID,EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID  union select topclientid,topclientname,ClientID,ClientFacilityID, EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,NULL, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID union select topclientid,topclientname,ClientID,ClientFacilityID, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID union select topclientid,topclientname,ClientID,ClientFacilityID, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, demo_one_label, NULL,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName, demo_one_label,ClientID,ClientFacilityID union select topclientid,topclientname,ClientID,ClientFacilityID, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, NULL, NULL,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName,ClientID,ClientFacilityID union select topclientid,topclientname,ClientID,NULL, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, NULL, NULL,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName,ClientID union select topclientid,topclientname,NULL,NULL, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, NULL, NULL,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName union select topclientid,NULL,NULL,NULL, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, NULL, NULL,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName union select NULL,NULL,NULL,NULL, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, NULL, NULL,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName").repartition(5000).cache()


//val a = sparksql.sql("select topclientid,topclientname,ClientID,ClientFacilityID,EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName,SurveyItemID, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID").toDF()
//val b = sparksql.sql("select topclientid,topclientname,ClientID,ClientFacilityID, EDWSourceFusionServiceID, EDWSourceFusionServiceName,SurveySectionID, SurveySectionName,NULL, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionID,SurveySectionName, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID")
//val c = sparksql.sql("select topclientid,topclientname,ClientID,ClientFacilityID, EDWSourceFusionServiceID, EDWSourceFusionServiceName,NULL, SurveySectionName,NULL, SurveyItemReportName, demo_one_label, demo_one_value,avg(SurveyResponseScore) as MeanScore,count(distinct(SurveyKey)) as nsize,100* (SUM(CASE WHEN surveymodename = 'Phone'  THEN CASE WHEN AdjustedSampleFlag = 1 THEN CASE WHEN PatientDischargeDateKey BETWEEN PatientDischargeStartDateKey AND PatientDischargeEndDateKey THEN SurveyResponseTopBoxCount + CAHPSTopBoxScoreCalibrationFactor ELSE SurveyResponseTopBoxCount END Else SurveyResponseTopBoxCount End Else SurveyResponseTopBoxCount END ) / sum(SurveyResponseCount)  ) as adj_phonetopbox from Service_table_as group by topclientid,topclientname,EDWSourceFusionServiceID,EDWSourceFusionServiceName,SurveySectionName, SurveyItemReportName, demo_one_label, demo_one_value ,ClientID,ClientFacilityID") 
 // a.unionAll(b union c)
//Service_table_as.collect.foreach(println)
GroupBy.collect.foreach(println)
 } 
   
}