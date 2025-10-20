package app;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;
import java.util.Map.Entry;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.time.Duration;
import java.util.Properties;
import java.time.Instant;
import java.time.Duration;
public class KafkaProcessor{
    private class LRUcache<K,V> extends LinkedHashMap<K,V>{
        private int capacity;
        public LRUcache(int buckets){
                super(buckets, 0.75f, true);
                this.capacity = buckets;
            }
        @Override
        protected boolean removeEldestEntry(Entry<K,V> eldest){
            return this.size() > this.capacity;
        }    
    }
    private class PlayerStats{
        public int points; 
        public Instant time;
        public PlayerStats(int points, Instant time ){
            this.points = points;
            this.time = time;
        }
        public void setPoints(int points, Instant time){
            this.points = points;
            this.time = time;
        }
        public float pointsPerSecond(int points, Instant time){
            long deltaN = Duration.between(this.time, time).toNanos();
            if(deltaN==0){
                return 0f;
            }else{
                return (float) ((double) (points-this.points)*1_000_000_000.0 / (double) deltaN);
            }
        }
    }
    private int lrumax = 10000;
    private LRUcache<Integer, PlayerStats> map = new LRUcache(lrumax);
    private int maxPPS = 5;
    private KafkaProducer<String, GenericRecord> producer;
    private Schema schema; 
    public static void main(String[] args){
       new KafkaProcessor(); 
    }
    public KafkaProcessor(){
        
        Properties p = new Properties();
        //https://kafka.apache.org/20/generate  d/consumer_config.html
        p.setProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"));
        p.setProperty("group.id", "Processors3");
        p.setProperty("auto.offset.reset", "latest");
        p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        p.setProperty("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        p.setProperty("security.protocol", "SASL_SSL");
        p.setProperty("sasl.mechanism", "PLAIN");
        //https://docs.confluent.io/platform/current/schema-registry/sr-client-configs.html#basic-auth-credentials-source
        //https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html#test-drive-avro-schema
        p.setProperty("sasl.jaas.config",
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
    + "username=\"" + System.getenv("API_KEY") + "\" "
    + "password=\"" + System.getenv("API_SECRET") + "\";");     
        p.setProperty("schema.registry.url", System.getenv("SR_URL"));
        p.setProperty("basic.auth.credentials.source", "USER_INFO");
        p.setProperty("basic.auth.user.info", System.getenv("SR_USER") + ":" + System.getenv("SR_PASSWORD"));
        p.setProperty("specific.avro.reader", "false"); // genericrecord
        //producer specific
        p.setProperty("acks", "all");
        
        this.schema = getSchema("PPS_reports");
        this.producer = new KafkaProducer<>(p);

        try(KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(p)){
            consumer.subscribe(Arrays.asList("gaming_activity"));
            while(true){
                final ConsumerRecords<String, GenericRecord> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String, GenericRecord> record : consumerRecords){
                    System.out.printf("%s, %s\n", record.key(), record.value());
                    recordHandler(record);
                }
            }
        }
    }
    private Schema getSchema(String topicname){
        SchemaRegistryClient sr = new CachedSchemaRegistryClient(System.getenv("SR_URL"), 128, Map.of("basic.auth.credentials.source", "USER_INFO", "basic.auth.user.info", System.getenv("SR_USER") + ":" + System.getenv("SR_PASSWORD")));
        topicname+="-value";
        try{return new Schema.Parser().parse(sr.getLatestSchemaMetadata(topicname).getSchema());}
        catch(Exception e){e.printStackTrace(); throw new IllegalStateException("Failed to load schema" + e);}
    }
    private void recordHandler(ConsumerRecord<String, GenericRecord> record){
        PlayerStats playerStats = map.get(Integer.valueOf(record.key()));
            int points = (Integer) record.value().get("points");
            Instant now = Instant.ofEpochMilli(record.timestamp());
        if(playerStats!=null){
            float playerPPS = playerStats.pointsPerSecond(points, now);
            playerStats.setPoints(points, now);
            if(playerPPS>this.maxPPS){
                //HANDLE ON REPORT TOPIC
                System.out.printf("User: %s is exceeding the max Points Per Second at %s, This instance has been logged", record.key(), playerPPS);
                GenericRecord value = new GenericData.Record(this.schema);
                value.put("pps", playerPPS);
                value.put("player_id", Integer.valueOf(record.key()));
                value.put("time", now.getEpochSecond());
                ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>("PPS_reports", record.key(), value);
                this.producer.send(producerRecord, (meta, e) -> {
                    if(e!=null) {
                        e.printStackTrace();
                    } else{
                        System.out.printf("Published to %s partition %s and offset %s%n", meta.topic(), meta.partition(), meta.offset());
                    }

                });
            }else{
                System.out.printf("User exists at %f PPS", playerPPS);
            }
            
        }else{
            map.put((Integer.valueOf(record.key())), new PlayerStats(points, now));
        } 
         
    }
}
