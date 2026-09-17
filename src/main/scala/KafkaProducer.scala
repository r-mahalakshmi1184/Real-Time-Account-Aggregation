import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object KafkaProducerApp {

  def main(args: Array[String]): Unit = {

    val props = new Properties()

    props.put("bootstrap.servers", "172.29.224.1:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val topic = "account-transactions"

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val baseTime = LocalDateTime.now()

    val transactions = Seq(
      ("ACC001", "CREDIT", 5000.0, baseTime),
      ("ACC001", "DEBIT", 1200.0, baseTime.plusMinutes(1)),
      ("ACC002", "CREDIT", 3000.0, baseTime.plusMinutes(2)),
      ("ACC002", "DEBIT", 500.0, baseTime.plusMinutes(3)),
      ("ACC001", "CREDIT", 1000.0, baseTime.plusMinutes(6))
    )

    transactions.foreach {
      case (accountId, transactionType, amount, timestamp) =>

        val formattedTimestamp = timestamp.format(formatter)

        val value =
          s"$accountId,$transactionType,$amount,$formattedTimestamp"

        val record = new ProducerRecord[String, String](
          topic,
          accountId,
          value
        )

        producer.send(record)
    }

    producer.flush()
    producer.close()

    println("Transactions sent successfully!")
  }
}
