package samples
import java.util.Properties
import kafka.producer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object Samp {
  
  def main(args: Array[String]) {
    //Kafka Server connection properties
    val properties = new Properties()
    properties.put("metadata.broker.list", "10.200.14.36:9092")
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    properties.put("request.required.acks", "1")
    val producerConfig = new ProducerConfig(properties)
    val producer = new Producer[String, String](producerConfig)
    for(i<- 1 to 50){
          //preparing keyedMessage class
          val message = new KeyedMessage[String,String](s"test","Hello PG Team --> "+i)
          //Producing Messages into kafka Server
          producer.send(message)
          //Holding thread for 1 second after pushing one message into kafka server
          Thread.sleep(2000)
      }
   }

}