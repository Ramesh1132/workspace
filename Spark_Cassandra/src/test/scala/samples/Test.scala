package samples
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils 
import org.apache.log4j.{Logger,Level}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector
import java.util.Date
import org.apache.spark.sql._


object Test {
  


case class stream4(id :String, email: String, f_name : String,l_name : String,gendr : String,salary : Float)
object Demo {
  def main(args : Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
val
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver") .set("spark.driver.allowMultipleContexts", "true").set("spark.cassandra.connection.host","localhost")        

val ssc = new StreamingContext(conf, Seconds(5))
   val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
 

val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("test" -> 1))




  val rdd =  kafkaStream.map(_._2) 
    //  .map(_.split(","))
      //val rdd1 = rdd.map(z => (z(0), z(1), z(2),z(3),z(4),z(5).toFloat))
      val rdd1 = rdd.map(x => x.split(","))
     
      //val a = kafkaStream.sparkContext.makeRDD(Seq("b", "c"))
      //a.collect().foreach(println);
      
     // .saveToCassandra("agregetion", "sample", SomeColumns("id","email","f_name", "gendr", "l_name","salary","percentrank"))// remove any empty words caused by double spaces
  ssc.start()
ssc.awaitTermination()







  }
}
}