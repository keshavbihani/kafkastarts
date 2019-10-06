package kafkastarts.learn1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class kafkaproducerwithkeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(kafkaproducerwithkeys.class);
        // create Producers properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092"); // kafka brokers

        //They basicaly let know what types of values you are sending to kafka
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer(props);
        for(int i=0;i<10;i++){
            // send data
            ProducerRecord<String, String> record = new ProducerRecord("first-topic","id_" + i,"hello world "+ i);

            // The send() is asynchronous and this method will return immediately once the record has been
            // stored in the buffer of records waiting to be sent. This allows sending many records in parallel
            // without blocking to wait for the response after each one.
            System.out.println(record.value());
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null)
                    {
                        logger.info(recordMetadata.topic()+" "+recordMetadata.partition()+" "+recordMetadata.timestamp());
                    }
                    else
                    {
                        logger.error("Error while producing",e);
                    }
                }
            }).get(); // dont do it in production , it will block the send method to make it syncronous.
        }

        // flush()
        //Invoking this method makes all buffered records immediately available to send (even if linger.ms is greater than 0) and
        // blocks on the completion of the requests associated with these records.
        producer.flush();
        //flush and close the producer
        producer.close();
    }
}
