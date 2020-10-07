package clientAssignment1Part1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.ResponseStat;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

public class SkierClientA1P1 {
    private final HttpClient client;

    public SkierClientA1P1() {
        this.client = new HttpClient();
    }

    public boolean executeGetRequest(String url) {
        HttpMethod httpGet = new GetMethod(url);

        try {
            // Execute HTTP GET request.
            int statusCode = this.client.executeMethod(httpGet);

            // Get HTTP response.
            String responseBody = httpGet.getResponseBodyAsString();

            // Deal with the response.
            System.out.printf("Retrieved response is: %s, with code %d, thread id is: %s\n\n",
                    responseBody,
                    statusCode,
                    Thread.currentThread().getId());
        } catch (IOException e) {
            System.out.printf("Failed to send GET request to %s, with error: %s\n\n", url, e);
            return false;
        } finally {
            httpGet.releaseConnection();
        }

        return true;
    }

    public boolean executePOSTRequest(String url, String bodyJsonStr) {
        PostMethod httpPost = new PostMethod(url);

        try {
            // Set request body content.
            StringRequestEntity entity = new StringRequestEntity(bodyJsonStr, "application/json", "UTF-8");
            httpPost.setRequestEntity(entity);

            // Execute HTTP POST request.
            int statusCode = this.client.executeMethod(httpPost);

            // Get HTTP response.
            String responseBody = httpPost.getResponseBodyAsString();

            // Deal with the response.
            System.out.printf("Retrieved response is: %s, with code %d, thread id is: %s\n\n",
                    responseBody,
                    statusCode,
                    Thread.currentThread().getId());
        } catch (IOException e) {
            System.out.printf("Failed to send POST request to %s, with error: %s\n\n", url, e);
            return false;
        } finally {
            httpPost.releaseConnection();
        }

        return true;
    }

    public static void main(String[] args) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        final String targetUrl = "http://ec2-54-92-222-44.compute-1.amazonaws.com:8080/IntelliJ_war/skiers/liftrides";

        // Prepare POST request body.
        ResponseStat mockStat = ResponseStat.builder()
                .requestStartTime(123l)
                .responseEndTime(567l)
                .threadId(166l)
                .encounteredError(false)
                .build();

        // Multi-threaded clientAssignment1Part1 usecase.
        runMultiThreadedPOSTCalls(targetUrl, 5, mapper.writeValueAsString(mockStat));
    }

    private static void runMultiThreadedPOSTCalls(String targetUrl,
                                                  int targetThreadCount,
                                                  String bodyJsonStr) {
        System.out.printf("Run multi-threaded POST calls toward %s\n", targetUrl);

        // Setup the ExecutorService (hence threads pool).
        ExecutorService executorService = Executors.newFixedThreadPool(targetThreadCount);

        // Initiate the requests.
        List<Future<Optional<ResponseStat>>> responses = sendMultiThreadedPOSTCalls(targetUrl,
                targetThreadCount,
                executorService,
                bodyJsonStr);

        // Process the responses.
        processMultiThreadedResponses(responses);

        // Close out ExecutorService.
        executorService.shutdown();
    }

    private static List<Future<Optional<ResponseStat>>> sendMultiThreadedPOSTCalls(String targetUrl,
                                                                                   int targetThreadCount,
                                                                                   ExecutorService executorService,
                                                                                   String bodyJsonStr) {
        // Setup the task definition.
        Callable<Optional<ResponseStat>> taskDefinition = () -> {
            SkierClientA1P1 threadedClient = new SkierClientA1P1();
            long startTime = System.currentTimeMillis();
            boolean executionResult = threadedClient.executePOSTRequest(targetUrl, bodyJsonStr);
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
