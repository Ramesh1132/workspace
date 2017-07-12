
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.UDTValue 

object LoadDataCassandra {
 



  
  
 def main(args: Array[String]) {
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("CounsumeData").setMaster("local[2]")
    val sc = new SparkContext(sparkConf);
    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(2))
    val topicsSet =List("test-sqlite-jdbc-info_by_system_hcacol").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "10.200.14.36:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStreamingContext, kafkaParams, topicsSet)
    // Get the lines, split them into words, count the words and print
    val RDD = messages.map(_._2).toDF();
   
    
 
//RDD.saveToCassandra("keyspace_name", "table_name", SomeColumns("col name", "col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name","col name",))
RDD.saveToCassandra("myKeySpace", "user", SomeColumns("surveyreceiveddatekey",
"patientdischargedatekey",
"surveyreceivedyear",
"surveyreceivedmonth",
"surveyreceivedday",
"clientid",
"clientname",
"system_hcacol",
"topclientid",
"topclientname",
"productname",
"serviceid",
"servicename",
"surveyname",
"surveymodename",
"surveysectionname",
"surveyitemreportname",
"surveyresponsevalue",
"surveyresponsetopboxcount",
"surveyresponsescore",
"surveyresponsecount",
"topboxsurveyitemresponseflag",
"edwsourcefusionservicename",
"surveykey",
"surveyitemskipflag",
"surveyitemcleanflag",
"surveyitemcmsflag",
"surveyitemstandardpgflag",
"surveyitemconversionflag",
"surveyitemcahpsflag",
"surveyitempganalyticflag",
"surveyitemmeancahpsflag",
"surveyitemrawflag",
"surveyitemrawviewflag",
"surveyitemcmsviewflag",
"surveyitemglobalflag",
"surveyitemscreenerflag",
"surveyitemaboutyouflag",
"surveyitemcmssuppressflag",
"adjustedsampleflag",
"surveyconsenttoshareflag",
"ignoredsurveyitemresponsevalueflag",
"invalidresponseflag"
));
 
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}

}