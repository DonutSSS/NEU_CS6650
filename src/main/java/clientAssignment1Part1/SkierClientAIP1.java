package clientAssignment1Part1;

import base.SkierClientBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.NonNull;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SkierClientAIP1 extends SkierClientBase {
    public SkierClientAIP1(final String serverIp,
                           int serverPort,
                           @NonNull Optional<Integer> maxThreadCount,
                           @NonNull Optional<Integer> skierCount,
                           @NonNull Optional<Integer> skiLiftCount,
                           @NonNull Optional<Integer> skiDayNum,
                           @NonNull Optional<String> resortName) {
        super(serverIp,
                serverPort,
                maxThreadCount,
                skierCount,
                skiLiftCount,
                skiDayNum,
                resortName);
    }

    public static void main(String[] args) throws JsonProcessingException, InterruptedException, ExecutionException {
        final String targetUrl = "http://ec2-54-92-222-44.compute-1.amazonaws.com:8080/IntelliJ_war/skiers/liftrides";

        SkierClientAIP1 client = new SkierClientAIP1(targetUrl,
                8080,
                Optional.of(256),
                Optional.of(20000),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        client.startLoadSimulation();
    }
}
