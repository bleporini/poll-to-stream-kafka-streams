package blep;

import io.vavr.concurrent.Future;
import io.vavr.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class slowService {

    final Timer timer = new Timer();
    final AtomicBoolean decider = new AtomicBoolean();

    public Future<String> sooooLong(String s){
        Promise<String> promise = Promise.make();
        log.info("Starting long task");

        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        log.info("Ending long task");
                        boolean decision = decider.get();
                        if (decision) {
                            log.info("PAID! ");
                            promise.success("PAID");
                        } else {
                            log.info("Not this time!");
                            promise.success("PENDING");
                        }
                        decider.set(!decision);
                    }
                },
                5000
        );

        return promise.future();
    }

}
