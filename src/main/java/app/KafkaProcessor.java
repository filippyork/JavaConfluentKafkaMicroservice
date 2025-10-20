package app;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.time.Duration;
import java.util.Properties;
public class KafkaProcessor{

    public static void main(String[] args){
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"));
        p.setProperty("group.id", "Processors1");
        p.setProperty("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        p.setProperty("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        p.setProperty("security.protocol", "SASL_SSL");
        p.setProperty("sasl.mechanism", "PLAIN");
        p.setProperty("sasl.jaas.config",
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
    + "username=\"" + System.getenv("API_KEY") + "\" "
    + "password=\"" + System.getenv("API_SECRET") + "\";");     
        p.setProperty("schema.registry.url", System.getenv("SR_URL"));
        p.setProperty("basic.auth.credentials.source", "USER_INFO");
        p.setProperty("basic.auth.user.info", System.getenv("SR_USER") + ":" + System.getenv("SR_PASSWORD"));
        p.setProperty("specific.avro.reader", "false"); // genericrecord

        try(KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(p)){
            consumer.subscribe(Arrays.asList("users-topic"));
            while(true){
                final ConsumerRecords<GenericRecord, GenericRecord> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<GenericRecord, GenericRecord> record : consumerRecords){
                    System.out.printf("%s, %s", record.key(), record.value());
                }
            }
        }
        
    }
}
