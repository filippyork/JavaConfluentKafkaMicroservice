package app;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.Record; 

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
    private Schema schema; 
    private final Serde<String> keySerde = Serdes.String(); //SERializer and DEserializer SERDE
    private final Serde<GenericRecord> valueSerde = new GenericAvroSerde();
    public static void main(String[] args){
       new KafkaProcessor(); 
    }
    public KafkaProcessor(){
       
        Properties p = new Properties();
        //https://kafka.apache.org/20/generate  d/consumer_config.html
        p.setProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"));
        p.setProperty("application.id", "StreamProcessors1");
        p.setProperty("auto.offset.reset", "latest");
        
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
        
                
        Map<String, Object> config = Map.of(
    "schema.registry.url", System.getenv("SR_URL"),
    "basic.auth.credentials.source", "USER_INFO",
    "basic.auth.user.info", System.getenv("SR_USER")+":"+System.getenv("SR_PASSWORD")
); 
        valueSerde.configure(config, false); 
        this.schema = getSchema("PPS_reports"); 

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> stream = builder.stream("gaming_activity", Consumed.with(keySerde, valueSerde));
        stream.process(() -> new Processor<String, GenericRecord, String, GenericRecord>(){
            private ProcessorContext<String, GenericRecord> ctx;
            @Override public void init(ProcessorContext<String, GenericRecord> context){
                this.ctx = context; // hold context for later forwarding
        }
            @Override public void process(Record<String, GenericRecord> record){
                GenericRecord newval = recordHandler(record);
                if(newval!=null)this.ctx.forward(record.withValue(newval));
            }

        }).to("PPS_reports", Produced.with(keySerde, valueSerde)); 
        KafkaStreams ks = new KafkaStreams(builder.build(), p);
        ks.start();
    }
    private Schema getSchema(String topicname){
        SchemaRegistryClient sr = new CachedSchemaRegistryClient(System.getenv("SR_URL"), 128, Map.of("basic.auth.credentials.source", "USER_INFO", "basic.auth.user.info", System.getenv("SR_USER") + ":" + System.getenv("SR_PASSWORD")));
        topicname+="-value";
        try{return new Schema.Parser().parse(sr.getLatestSchemaMetadata(topicname).getSchema());}
        catch(Exception e){e.printStackTrace(); throw new IllegalStateException("Failed to load schema" + e);}
    }
    private GenericRecord recordHandler(Record<String, GenericRecord> record){
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
                return value;
                }
             else{
                System.out.printf("User exists at %f PPS", playerPPS);
            }
            
        }else{
            map.put((Integer.valueOf(record.key())), new PlayerStats(points, now));
        } 
        return null;
         
    }
}
