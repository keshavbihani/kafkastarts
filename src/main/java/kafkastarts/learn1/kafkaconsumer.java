package kafkastarts.learn1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class kafkaconsumer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(kafkaconsumer.class.getName());
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        // it will converts the bytes send by prducer to string again using deserailizer
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        // this property is used to read from certain offset in partition. earliest/latest/none
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // all the consumer belongs to following consumer group
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-fourth-application");


        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        //subscribe to topic(s)
        consumer.subscribe(Arrays.asList("first-topic"));

        // poll data from topic
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                logger.info(record.key());
                logger.info(record.value());
            }
        }
    }
}
