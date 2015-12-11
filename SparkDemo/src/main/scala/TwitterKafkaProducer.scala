/**
 * Created by sachin on 12/10/15.
 */

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{TwitterStreamFactory, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TwitterKafkaProducer {

  def main(args: Array[String]) {
    val BROKER_LIST: String = "localhost:9092,localhost:9093,localhost:9094";
    val SERIALIZER: String = "kafka.serializer.StringEncoder";
    val REQUIRED_ACKS: String = "1";
    val KAFKA_TOPIC: String = "test1";

    /** Producer properties **/
    var props: Properties = new Properties()
    props.put("metadata.broker.list", BROKER_LIST)
    props.put("serializer.class", SERIALIZER)
    props.put("request.required.acks", REQUIRED_ACKS)

    val config: ProducerConfig = new ProducerConfig(props)
    val producer: Producer[String, String] = new Producer[String, String](config)


    def simpleStatusListener = new StatusListener() {
      def onStatus(status: Status) {
        val data: KeyedMessage[String, String] = new KeyedMessage[String, String](KAFKA_TOPIC, status.toString);

        val tasks = for (i <- 1 to 1000) yield Future {
          println("Executing task " + i)
          producer.send(data)
        }
      }

      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}

      def onException(ex: Exception) {
        ex.printStackTrace
      }

      def onScrubGeo(arg0: Long, arg1: Long) {}

      def onStallWarning(warning: StallWarning) {}
    }

    val CONSUMER_KEY_KEY: String = "G6d9Tl0Hh0uA3RC4Sx0Cg81tk";
    val CONSUMER_SECRET_KEY: String = "i5tjc4EuavhEuxO5vzDpgTb57wkyX66QmgkwZIZCOXW8LTG3C2";
    val ACCESS_TOKEN_KEY: String = "145001241-8ldS6p1Ci6HDk0PV9mk3c5ldnDSHzNBe6HK3FS31";
    val ACCESS_TOKEN_SECRET_KEY: String = "NbciwyIuQr3NHOqrQXNGHuenWuB163hOLcDF6FAAbZNNv";

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET_KEY)
      .setOAuthAccessToken(ACCESS_TOKEN_KEY)
      .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET_KEY)

    val twitterStream = new TwitterStreamFactory(cb.build()).getInstance
    twitterStream.addListener(simpleStatusListener)
    val sampleStream = twitterStream.getSampleStream
    do {
      sampleStream.next(simpleStatusListener)
    } while (true)

    Thread.sleep(2000)
    twitterStream.cleanUp
    twitterStream.shutdown

  }
}
