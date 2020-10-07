package clientAssignment1Part2;

import model.ResponseStat;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SkierClientA1P2 {
    private final HttpClient client;

    public SkierClientA1P2() {
        this.client = new HttpClient();
    }

    public boolean executeGetRequest(String url) {
        HttpMethod httpGet = new GetMethod(url);

        try {
            // Execute HTTP Get request.
            this.client.executeMethod(httpGet);

            // Get HTTP response.
            String responseBody = httpGet.getResponseBodyAsString();

            // Deal with the response.
            System.out.printf("Retrieved response is: %s, thread id is: %s\n\n", responseBody, Thread.currentThread().getId());
        } catch (IOException e) {
            System.out.printf("Failed to process Get request to %s, with error: %s\n\n", url, e);
            return false;
        } finally {
            httpGet.releaseConnection();
        }

        return true;
    }

    public static void main(String[] args) {
        final String targetUrl = "http://ec2-54-92-222-44.compute-1.amazonaws.com:8080/IntelliJ_war/hello";
        // Simple clientAssignment1Part1 usecase.
        runSimpleClient(targetUrl);

        // Multi-threaded clientAssignment1Part1 usecase.
        runMultiThreadedClients(targetUrl, 5);
    }

    private static void runSimpleClient(String targetUrl) {
        System.out.printf("Run simple single threaded clientAssignment1Part1 toward %s\n\n", targetUrl);
        SkierClientA1P2 client = new SkierClientA1P2();
        client.executeGetRequest(targetUrl);
    }

    private static void runMultiThreadedClients(String targetUrl, int targetThreadCount) {
        System.out.printf("Run multi-threaded clients toward %s\n", targetUrl);

        // Setup the ExecutorService (hence threads pool).
        ExecutorService executorService = Executors.newFixedThreadPool(targetThreadCount);

        // Initiate the requests.
        List<Future<Optional<ResponseStat>>> responses = sendMultiThreadedCalls(targetUrl, targetThreadCount, executorService);

        // Process the responses.
        processMultiThreadedResponses(responses);

        // Close out ExecutorService.
        executorService.shutdown();
    }

    private static List<Future<Optional<ResponseStat>>> sendMultiThreadedCalls(String targetUrl,
                                                                               int targetThreadCount,
                                                                               ExecutorService executorService) {
        // Setup the task definition.
        Callable<Optional<ResponseStat>> taskDefinition = () -> {
            SkierClientA1P2 threadedClient = new SkierClientA1P2();
            long startTime = System.currentTimeMillis();
            boolean executionResult = threadedClient.executeGetRequest(targetUrl);
            long endTime = System.currentTimeMillis();

            ResponseStat stat;
            if (executionResult) {
                stat = ResponseStat.builder()
                        .requestStartTime(startTime)
                        .responseEndTime(endTime)
                        .threadId(Thread.currentThread().getId())
                        .encounteredError(false)
                        .build();
            } else {
                stat = ResponseStat.builder()
                        .requestStartTime(startTime)
                        .responseEndTime(endTime)
                        .threadId(Thread.currentThread().getId())
                        .encounteredError(true)
                        .build();
            }

            return Optional.of(stat);
        };

        // Setup the tasks.
        List<Callable<Optional<ResponseStat>>> tasks = new ArrayList<>();
        for (int i = 0; i < targetThreadCount; i++) {
            tasks.add(taskDefinition);
        }

        // Now execute the tasks in parallel.
        List<Future<Optional<ResponseStat>>> futures = new ArrayList<>();
        try {
            futures = executorService.invokeAll(tasks);
        } catch (InterruptedException e) {
            System.out.printf("Failed to retrieve threaded responses %s\n", e);
        }

        return futures;
    }

    private static void processMultiThreadedResponses(List<Future<Optional<ResponseStat>>> responses) {
        // Waiting for all requests to be completed.
        while (!responses.stream().allMatch(stat -> stat.isDone())) {
            System.out.println("Waiting for all requests to complete...");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.printf("Encountered error during wait sleep %s\n", e);
            }
        }

        // All requests should now be completed.
        for (Future<Optional<ResponseStat>> response : responses) {
            try {
                Optional<ResponseStat> statHolder = response.get();
                if (statHolder.isPresent()) {
                    ResponseStat stat = statHolder.get();
                    long timeTaken = stat.getResponseEndTime() - stat.getRequestStartTime();

                    System.out.printf("Response in thread %s took %d milliseconds in total, and has %s error\n",
                            stat.getThreadId(),
                            timeTaken,
                            (stat.isEncounteredError()) ? "encountered" : "no");
                } else {
                    System.out.printf("Encountered unexpected empty response\n");
                    continue;
                }
            } catch (Exception e) {
                System.out.printf("Encountered error while getting response %s\n", e);
                continue;
            }
        }
    }
}
