package blep;

import io.vavr.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.vavr.concurrent.Future.fromJavaFuture;

@Slf4j
public class Poller<K,P,V> {

    private final KafkaProducer<K, Retryable<P>> retryableProducer;

    private final KafkaProducer<K, V> responseProducer;

    private final Serde<K> keySerde;
    private final Serde<Retryable<P>> payloadSerde;

    private final String requestsTopic;
    private final String responsesTopic;
    private final String rejectionsTopic;

    private final Function<P, Future<V>> processor;
    private final Predicate<V> valueChecker;

    public Poller(
            KafkaProducer<K, Retryable<P>> retryableProducer,
            KafkaProducer<K, V> responseProducer,
            Serde<K> keySerde,
            Serde<Retryable<P>> payloadSerde,
            String requestsTopic,
            String responsesTopic,
            String rejectionsTopic,
            Function<P, Future<V>> processor,
            Predicate<V> valueChecker) {
        this.retryableProducer = retryableProducer;
        this.responseProducer = responseProducer;
        this.keySerde = keySerde;
        this.payloadSerde = payloadSerde;
        this.requestsTopic = requestsTopic;
        this.responsesTopic = responsesTopic;
        this.rejectionsTopic = rejectionsTopic;
        this.processor = processor;
        this.valueChecker = valueChecker;
    }


    public Topology buildTopology(StreamsBuilder builder) {

        builder.table(
                requestsTopic,
                Consumed.with(keySerde, payloadSerde)
        ).filter((id, v) ->
                v.canTry()
        ).mapValues(this::process);

        return builder.build();
    }

    private Future<RecordMetadata> process(K id, Retryable<P> retryable) {
        Retryable<P> tr1 = retryable.retry();
        return tr1.canTry() ?
                processor.apply(retryable.getPayload())
                        .flatMap(v ->
                                valueChecker.test(v) ?
                                        send(id, tr1.success(), v) :
                                        sendRetry(id, tr1)
                        ) :
                reject(id, tr1.failed());
    }

    private Future<RecordMetadata> sendRetry(K id, Retryable<P> retryable){
        return fromJavaFuture(
                retryableProducer.send(new ProducerRecord<>(requestsTopic, id, retryable))
        ) ;
    }

    private Future<RecordMetadata> send(K id, Retryable<P> value, V response) {
        return Future.reduce(
                Arrays.asList(
                        fromJavaFuture(
                                responseProducer.send(new ProducerRecord<>(responsesTopic, id, response))
                        ),
                        fromJavaFuture(
                                retryableProducer.send(new ProducerRecord<>(requestsTopic, id, value))
                        )
                ),
                (f1, f2) -> f2
        );
    }

    private Future<RecordMetadata> reject(K id, Retryable<P> retryable) {
        log.info("Rejecting id='{}'", id);
        return Future.reduce(
                Arrays.asList(
                        fromJavaFuture(
                                retryableProducer.send(new ProducerRecord<>(rejectionsTopic, id, retryable))
                        ),
                        fromJavaFuture(
                                retryableProducer.send(new ProducerRecord<>(requestsTopic, id, retryable))
                        )
                ),
                (f1, f2) -> f2
        );
    }

}
