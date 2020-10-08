package base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import model.RequestBody;
import model.ResponseStat;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public abstract class SkierClientBase {
    private static final int skiDayLenInMin = 420;

    private final MultiThreadedHttpConnectionManager connectionManager;
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final String serverIp;
    private final int serverPort;

    // Fields with default values.
    private int maxThreadCount = 8;
    private int skierCount = 50;
    private int skiLiftCount = 4;
    private int skiDayNum = 1;
    private String resortName = "SilverMt";

    private final int[] skiLifts;

    private int successfulRequestCount;
    private int failedRequestCount;

    public SkierClientBase(final String serverIp,
                           int serverPort,
                           @NonNull Optional<Integer> maxThreadCount,
                           @NonNull Optional<Integer> skierCount,
                           @NonNull Optional<Integer> skiLiftCount,
                           @NonNull Optional<Integer> skiDayNum,
                           @NonNull Optional<String> resortName) {
        this.connectionManager = new MultiThreadedHttpConnectionManager();
        this.client = new HttpClient(connectionManager);
        this.mapper = new ObjectMapper();
        this.serverIp = serverIp;
        this.serverPort = serverPort;

        // Set values if present.
        maxThreadCount.ifPresent(value -> this.maxThreadCount = value);
        skierCount.ifPresent(value -> this.skierCount = value);
        skiLiftCount.ifPresent(value -> this.skiLiftCount = value);
        skiDayNum.ifPresent(value -> this.skiDayNum = value);
        resortName.ifPresent(value -> this.resortName = value);

        this.skiLifts = getLiftRides();

        this.successfulRequestCount = 0;
        this.failedRequestCount = 0;
    }

//    public void processConcurrentPOSTRequests(String targetUrl,
//                                              int targetThreadCount,
//                                              String[] bodyJsonStr) {
//        System.out.printf("Run multi-threaded POST calls toward %s\n", targetUrl);
//
//        // Setup the ExecutorService (hence threads pool).
//        ExecutorService executorService = Executors.newFixedThreadPool(targetThreadCount);
//
//        // Initiate the requests.
//        List<Future<Optional<ResponseStat>>> responses = getPOSTRequests(targetUrl,
//                executorService,
//                bodyJsonStr);
//
//        // Process the responses.
//        processResponses(responses);
//
//        // Close out ExecutorService.
//        executorService.shutdown();
//    }

    public boolean executeSingleGetRequest(String targetUrl) {
        HttpMethod httpGet = new GetMethod(targetUrl);

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

            updateStatCounts(statusCode);
        } catch (IOException e) {
            System.out.printf("Failed to send GET request to %s, with error: %s\n\n", targetUrl, e);
            return false;
        } finally {
            httpGet.releaseConnection();
        }

        return true;
    }

    public boolean executeSinglePOSTRequest(String targetUrl, String bodyJsonStr) {
        PostMethod httpPost = new PostMethod(targetUrl);

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

            updateStatCounts(statusCode);
        } catch (IOException e) {
            System.out.printf("Failed to send POST request to %s, with error: %s\n\n", targetUrl, e);
            return false;
        } finally {
            httpPost.releaseConnection();
        }

        return true;
    }

    public void startLoadSimulation() throws JsonProcessingException, InterruptedException, ExecutionException {
        System.out.printf("Start skier client load simulation\n");

        long startTime = System.currentTimeMillis();
        executePhaseOne();
        long endTime = System.currentTimeMillis();

        System.out.printf("\n[Execution Summary]\nSuccessful Requests: %d\nFailed Requests: %d\nWall Time: %d seconds\nThroughput: %d rps\n",
                this.successfulRequestCount,
                this.failedRequestCount,
                (endTime - startTime) / 1000,
                (this.successfulRequestCount + this.failedRequestCount) / ((endTime - startTime) / 1000));
    }

    private void executePhaseOne() throws JsonProcessingException, InterruptedException, ExecutionException {
        // Setup requirements in phase 1.
        int targetThreadCount = this.maxThreadCount / 4;
        int[][] skierSplits = getSkierIdsSplits(targetThreadCount);
        int[] timeSlides = getTimeSlides(1);

        executePhase(1, targetThreadCount, skierSplits, timeSlides, 100, 5);
    }

    private void executePhaseTwo() throws InterruptedException, ExecutionException, JsonProcessingException {
        // Setup requirements in phase 2.
        int targetThreadCount = this.maxThreadCount;
        int[][] skierSplits = getSkierIdsSplits(targetThreadCount);
        int[] timeSlides = getTimeSlides(2);

        executePhase(2, targetThreadCount, skierSplits, timeSlides, 100, 5);
    }

    private void executePhaseThree() throws InterruptedException, ExecutionException, JsonProcessingException {
        // Setup requirements in phase 3.
        int targetThreadCount = this.maxThreadCount / 4;
        int[][] skierSplits = getSkierIdsSplits(targetThreadCount);
        int[] timeSlides = getTimeSlides(3);

        executePhase(3, targetThreadCount, skierSplits, timeSlides, 100, 10);
    }

    private void executePhase(int phaseNum,
                              int targetThreadCount,
                              int[][] skierSplits,
                              int[] timeSlides,
                              int targetPOSTRequestCount,
                              int targetGETRequestCount) throws InterruptedException, ExecutionException, JsonProcessingException {
        System.out.printf("Execute phase %d\n", phaseNum);

        // Setup the ExecutorService.
        ExecutorService executorService = Executors.newFixedThreadPool(targetThreadCount);

        // Send desired POST requests.
        sendLoads(targetThreadCount,
                skierSplits,
                timeSlides,
                executorService,
                true,
                targetPOSTRequestCount,
                (phaseNum == 1) ? true : false,
                (phaseNum == 2) ? true : false);

        // Send desired GET requests.
        sendLoads(targetThreadCount,
                skierSplits,
                timeSlides,
                executorService,
                false,
                targetGETRequestCount,
                (phaseNum == 1) ? true : false,
                (phaseNum == 2) ? true : false);

        // Close out ExecutorService.
        executorService.shutdown();
    }

    private void sendLoads(int targetThreadCount,
                           int[][] skierSplits,
                           int[] timeSlides,
                           ExecutorService executorService,
                           boolean isPostRequest,
                           int targetRequestCount,
                           boolean isPhaseOne,
                           boolean isPhaseTwo) throws JsonProcessingException, InterruptedException, ExecutionException {
        // The finalized concurrent tasks list for POST requests.
        List<Callable<Optional<ResponseStat>>> tasks = new ArrayList<>();

        // Sending the POST requests in batch toward the threads pool.
        for (int i = 0; i < targetThreadCount; i++) {
            if (isPostRequest) {
                // Prepare POST requests for current thread.
                String[] proposedPOSTRequestBody = preparePOSTRequestsForThread(targetRequestCount, skierSplits[i], timeSlides);

                // Append to the finalized tasks list.
                tasks.addAll(getPOSTRequests(this.serverIp, proposedPOSTRequestBody));
            } else {
                // Prepare GET requests for current thread.
                String[] proposedGETRequests = prepareGETRequestsForThread(targetRequestCount, skierSplits[i]);

                // Append to the finalized tasks list.
                tasks.addAll(getGETRequests(proposedGETRequests));
            }
        }

        // Now execute all the tasks concurrently.
        List<Future<Optional<ResponseStat>>> responses = executorService.invokeAll(tasks);

        // Process the responses.
        if (isPhaseOne) {
            processResponses(responses, true, false, targetRequestCount, targetThreadCount);
        } else if (isPhaseTwo) {
            processResponses(responses, false, true, targetRequestCount, targetThreadCount);
        } else {
            processResponses(responses, false, false, targetRequestCount, targetThreadCount);
        }
    }

    private boolean sendSerialPOSTRequests(String targetUrl,
                                        String[] requestBody) {
        boolean executionResult = true;

        // Execute all tasks within a single Thread.
        List<Callable<Optional<ResponseStat>>> tasks = new ArrayList<>();
        for (int i = 0; i < requestBody.length; i++) {
            executionResult &= executeSinglePOSTRequest(targetUrl, requestBody[i]);
        }

        return executionResult;
    }

    private boolean sendSerialGETRequests(String[] targetUrls) {
        boolean executionResult = true;

        // Execute all tasks within a single Thread.
        for (int i = 0; i < targetUrls.length; i++) {
            executionResult &= executeSingleGetRequest(targetUrls[i]);
        }

        return executionResult;
    }

    private List<Callable<Optional<ResponseStat>>> getPOSTRequests(String targetUrl,
                                                                   String[] requestBody) {
        // Setup the tasks.
        List<Callable<Optional<ResponseStat>>> tasks = new ArrayList<>();
        tasks.add(() -> {
            long startTime = System.currentTimeMillis();
            boolean executionResult = sendSerialPOSTRequests(targetUrl, requestBody);
            long endTime = System.currentTimeMillis();

            ResponseStat.ResponseStatBuilder statBuilder = ResponseStat.builder()
                    .requestStartTime(startTime)
                    .responseEndTime(endTime)
                    .threadId(Thread.currentThread().getId());
            if (executionResult) {
                statBuilder.encounteredError(false);
            } else {
                statBuilder.encounteredError(true);
            }

            return Optional.of(statBuilder.build());
        });

        return tasks;
    }

    private List<Callable<Optional<ResponseStat>>> getGETRequests(String[] targetUrls) {
        // Setup the tasks.
        List<Callable<Optional<ResponseStat>>> tasks = new ArrayList<>();
        tasks.add(() -> {
            long startTime = System.currentTimeMillis();
            boolean executionResult = sendSerialGETRequests(targetUrls);
            long endTime = System.currentTimeMillis();

            ResponseStat.ResponseStatBuilder statBuilder = ResponseStat.builder()
                    .requestStartTime(startTime)
                    .responseEndTime(endTime)
                    .threadId(Thread.currentThread().getId());
            if (executionResult) {
                statBuilder.encounteredError(false);
            } else {
                statBuilder.encounteredError(true);
            }

            return Optional.of(statBuilder.build());
        });

        return tasks;
    }

    private void processResponses(List<Future<Optional<ResponseStat>>> responses,
                                  boolean isPhaseOne,
                                  boolean isPhaseTwo,
                                  int targetRequestCountPerThread,
                                  int targetThreadCount) throws ExecutionException, InterruptedException, JsonProcessingException {
        boolean isPhaseTwoExecuted = false, isPhaseThreeExecuted = false;

        // Waiting for all requests to be completed.
        while (!responses.stream().allMatch(stat -> stat.isDone())) {
            System.out.println("Waiting for all requests to complete...");

            // Check if at least 10% of the threads complete their works.
            List<Future<Optional<ResponseStat>>> completedRequests = responses.stream()
                    .filter(stat -> stat.isDone()).collect(Collectors.toList());

            // If so, execute the next phase.
            if (isPhaseOne || isPhaseTwo) {
                if (getCompletedThreadCount(completedRequests, targetRequestCountPerThread) > (targetThreadCount / 10)) {
                    if (isPhaseOne) {
                        isPhaseTwoExecuted = true;
                        executePhaseTwo();
                    } else if (isPhaseTwo) {
                        isPhaseThreeExecuted = true;
                        executePhaseThree();
                    }
                }
            }

            // Then wait for all requests to complete before further processing the responses.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.printf("Encountered error during wait sleep %s\n", e);
            }
        }

        if (isPhaseOne && !isPhaseTwoExecuted) {
            executePhaseTwo();
        }
        if (isPhaseTwo && !isPhaseThreeExecuted) {
            executePhaseThree();
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

    private int getCompletedThreadCount(List<Future<Optional<ResponseStat>>> completedTasks, int targetRequestCountPerThread) throws ExecutionException, InterruptedException {
        Map<Long, Integer> threadIdToCompletedRequestCount = new HashMap<>();

        for (Future<Optional<ResponseStat>> task : completedTasks) {
            if (task.get().isPresent()) {
                ResponseStat stat = task.get().get();

                Integer threadTaskCount = threadIdToCompletedRequestCount.computeIfAbsent(stat.getThreadId(), k -> Integer.valueOf(0));
                threadIdToCompletedRequestCount.put(stat.getThreadId(), new Integer(threadTaskCount.intValue() + 1));
            }
        }

        return threadIdToCompletedRequestCount.entrySet().stream().filter(k -> k.getValue() > targetRequestCountPerThread).collect(Collectors.toList()).size();
    }

    private int[][] getSkierIdsSplits(int targetThreadCount) {
        int skiersInEachSplit = this.skierCount / targetThreadCount;
        int[][] skierSplits = new int[targetThreadCount][skiersInEachSplit];

        int skierId = 1;
        for (int i = 0; i < targetThreadCount; i++) {
            for (int j = 0; j < skiersInEachSplit; j++) {
                skierSplits[i][j] = skierId++;
            }
        }
        return skierSplits;
    }

    private int[] getTimeSlides(int phaseNum) {
        int[] timeSlides;
        int initialTime;

        switch (phaseNum) {
            case 1:
                timeSlides = new int[90];
                initialTime = 1;
                break;
            case 2:
                timeSlides = new int[270];
                initialTime = 91;
                break;
            case 3:
                timeSlides = new int[60];
                initialTime = 361;
                break;
            default:
                throw new RuntimeException("Unsupported phase: " + phaseNum);
        }

        fillTimeSlides(timeSlides, initialTime);

        return timeSlides;
    }

    private void fillTimeSlides(int[] timeSlidesToFill, int initialTime) {
        for (int i = 0; i < timeSlidesToFill.length; i++) {
            timeSlidesToFill[i] = initialTime;
            initialTime++;
        }
    }

    private int[] getLiftRides() {
        int[] lifts = new int[this.skiLiftCount];

        for (int i = 0; i < this.skiLiftCount; i++) {
            lifts[i] = i + 1;
        }

        return lifts;
    }

    private String[] preparePOSTRequestsForThread(int targetRequestCount,
                                                  int[] skierSplit,
                                                  int[] timeSlides) throws JsonProcessingException {
        String[] requestBody = new String[targetRequestCount];

        // Shuffle the inputs.
        shuffleArray(skierSplit);
        shuffleArray(timeSlides);
        shuffleArray(this.skiLifts);

        // Generate request body based on the shuffled inputs.
        int i = 0, j = 0, k = 0, l = 0;
        while (l < targetRequestCount) {
            if (i >= skierSplit.length) {
                i = 0;
            }
            if (j >= timeSlides.length) {
                j = 0;
            }
            if (k >= this.skiLifts.length) {
                k = 0;
            }

            RequestBody body = RequestBody.builder()
                    .resortID(this.resortName)
                    .dayID(this.skiDayNum)
                    .skierID(skierSplit[i++])
                    .time(timeSlides[j++])
                    .liftID(this.skiLifts[k++])
                    .build();

            requestBody[l] = mapper.writeValueAsString(body);

            l++;
        }

        return requestBody;
    }

    private String[] prepareGETRequestsForThread(int targetRequestCount,
                                                 int[] skierSplit) {
        String[] requests = new String[targetRequestCount];

        // Shuffle the inputs.
        shuffleArray(skierSplit);

        // Generate requests based on the shuffle inputs.
        int i = 0, j = 0;
        while (j < targetRequestCount) {
            if (i >= skierSplit.length) {
                i = 0;
            }

            StringBuilder targetUrl = new StringBuilder();
            targetUrl.append("http://ec2-54-92-222-44.compute-1.amazonaws.com:8080/IntelliJ_war/skiers/");
            targetUrl.append(this.resortName);
            targetUrl.append("/days/");
            targetUrl.append(this.skiDayNum);
            targetUrl.append("/skiers/");
            targetUrl.append(skierSplit[i]);

            requests[j] = targetUrl.toString();

            i++;
            j++;
        }

        return requests;
    }

    private void shuffleArray(int[] arrayToShuffle) {
        Random rand = new Random();

        for (int i = 0; i < arrayToShuffle.length; i++) {
            int randomIndexToSwap = rand.nextInt(arrayToShuffle.length);
            int tmp = arrayToShuffle[randomIndexToSwap];

            arrayToShuffle[randomIndexToSwap] = arrayToShuffle[i];
            arrayToShuffle[i] = tmp;
        }
    }

    private void updateStatCounts(int statusCode) {
        if (statusCode == 200 || statusCode == 201) {
            this.successfulRequestCount++;
        } else {
            this.failedRequestCount++;
        }
    }
}
