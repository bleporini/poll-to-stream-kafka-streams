package blep;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class RetryableStringSerde implements Serde<Retryable<String>> {

    private static final Gson gson = new Gson();

    public static  class RetryableSerializer implements Serializer<Retryable<String>>{
        @Override
        public byte[] serialize(String topic, Retryable<String> data) {
            return gson.toJson(data).getBytes();
        }
    }

    public static class RetryableDeserializer implements Deserializer<Retryable<String>> {

        @Override
        public Retryable<String> deserialize(String topic, byte[] data) {
            JsonObject json = gson.fromJson(new String(data), JsonObject.class);
            String payload = json.get("payload").getAsString();
            int max = json.get("max").getAsInt();
            int tries = json.get("tries").getAsInt();
            String status = json.get("status").getAsString();
            return new Retryable<>(
                    payload,
                    max,
                    tries,
                    Retryable.Status.valueOf(status)
            );
        }
    }

    @Override
    public Serializer<Retryable<String>> serializer() {
        return new RetryableSerializer();
    }

    @Override
    public Deserializer<Retryable<String>> deserializer() {
        return new RetryableDeserializer();
    }

}
