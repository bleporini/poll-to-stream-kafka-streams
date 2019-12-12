package blep;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class PollerIntegrationTest {

    private final KafkaProducer<String, Retryable<String>> retryableProducer =
            new KafkaProducer<>(
                    Main.configure(),
                    new StringSerializer(),
                    new RetryableStringSerde.RetryableSerializer()
            );



    @Test
    public void send() throws ExecutionException, InterruptedException {
        retryableProducer.send(
                new ProducerRecord<>(
                        "requests",
                        "test",
                        Retryable.init("TO RUN", 1)
                )
        ).get();
    }

}