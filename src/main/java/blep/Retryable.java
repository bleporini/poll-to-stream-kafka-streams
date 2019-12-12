package blep;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import static blep.Retryable.Status.FAILED;
import static blep.Retryable.Status.SUCCEEDED;
import static blep.Retryable.Status.TRYING;

@AllArgsConstructor
@Getter
@ToString
public class Retryable<V> {

    private final V payload;

    private final Integer max;

    private final Integer tries;

    private final Status status;

    public Retryable<V> failed() {
        return new Retryable<>(
                payload,
                max,
                tries,
                FAILED
        );
    }

    public enum Status{
        TRYING, SUCCEEDED, FAILED
    }

    public Retryable<V> retry(){
        return new Retryable<>(
                payload,
                max,
                tries +1,
                max > tries ? TRYING : FAILED
        );
    }

    public Retryable<V> success(){
        return new Retryable<>(
                payload,
                max,
                tries,
                SUCCEEDED
        );
    }

    public boolean canTry() {
        return status == TRYING ;
    }

    public static <V> Retryable<V> init(V payload, Integer max) {
        return new Retryable<>(payload, max, 0, TRYING);
    }

}
