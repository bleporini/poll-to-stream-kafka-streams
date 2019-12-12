package blep;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Main {

    static Properties configure(){
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "poller");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        return props;
    }


    public static void main(String[] args) {
        Properties properties = configure();


        final KafkaProducer<String, Retryable<String>> retryableProducer =
                new KafkaProducer<>(
                        properties,
                        new StringSerializer(),
                        new RetryableStringSerde.RetryableSerializer()
                );


        final KafkaProducer<String, String> responseProducer =
                new KafkaProducer<>(
                        properties,
                        new StringSerializer(),
                        new StringSerializer()
                );

        final StreamsBuilder builder = new StreamsBuilder();

        slowService slowService = new slowService();

        Topology topology = new Poller<>(
                retryableProducer,
                responseProducer,
                Serdes.String(),
                new RetryableStringSerde(),
                "requests",
                "responses",
                "rejections",
                slowService::sooooLong,
                "PAID"::equals
        ).buildTopology(builder);

        final KafkaStreams streams = new KafkaStreams(topology, properties);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    streams.close();
                    latch.countDown();
                },
                "streams-shutdown")
        );

        try {
            streams.start();
        }catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }


    }
}
