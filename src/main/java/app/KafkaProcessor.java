package app;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.avro.generic.GenericRecord;

import java.util.LinkedHashMap;
import java.util.Arrays;
import java.time.Duration;
import java.util.Properties;
import java.time.Instant;
import java.time.Duration;
public class KafkaProcessor{
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
    private LinkedHashMap<Integer, PlayerStats> map;
    private int lrumax = 10000;
    private int maxPPS = 2;
    public static void main(String[] args){
       new KafkaProcessor(); 
    }
    public KafkaProcessor(){
        
        this.map = new LinkedHashMap<Integer, PlayerStats>(16, 0.75f, true); //LRU STYLE
        Properties p = new Properties();
        //https://kafka.apache.org/20/generate  d/consumer_config.html
        p.setProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"));
        p.setProperty("group.id", "Processors3");
        p.setProperty("auto.offset.reset", "latest");
        p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.setProperty("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
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
    private void recordHandler(ConsumerRecord<String, GenericRecord> record){
        PlayerStats playerStats = map.get(Integer.valueOf(record.key()));
            int points = (Integer) record.value().get("points");
            Instant now = Instant.now();
        if(playerStats!=null){
            float playerPPS = playerStats.pointsPerSecond(points, now);
            playerStats.setPoints(points, now);
            if(playerPPS>this.maxPPS){
                //HANDLE ON REPORT TOPIC
                System.out.printf("User: %s is exceeding the max Points Per Second at %s, This instance has been logged", record.key(), playerPPS);
            }else{
                System.out.printf("User exists at %f PPS", playerPPS);
            }
            
        }else{
            map.put((Integer.valueOf(record.key())), new PlayerStats(points, now));
            if(map.size()>lrumax){
                // POP LAST IF EXCEEDING LRU LIMIT
            }

        } 
         
    }
}
