package kafkastarts.learn1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class kafkaproducer {
    public static void main(String[] args) {
        // create Producers properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092"); // kafka brokers

        //They basicaly let know what types of values you are sending to kafka
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer(props);

        // send data
        ProducerRecord<String, String> record = new ProducerRecord("first-topic","hello world");

        // The send() is asynchronous and this method will return immediately once the record has been
        // stored in the buffer of records waiting to be sent. This allows sending many records in parallel
        // without blocking to wait for the response after each one.
        System.out.println(record.value());
        producer.send(record);

        // flush()
        //Invoking this method makes all buffered records immediately available to send (even if linger.ms is greater than 0) and
        // blocks on the completion of the requests associated with these records.
        producer.flush();
        //flush and close the producer
        producer.close();
    }
}
