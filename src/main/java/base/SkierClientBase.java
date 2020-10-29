package base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.SkierPOSTRequest;
import model.TaskResponseStat;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.log4j.Logger;
import utility.AWSUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class SkierClientBase {
    final Logger logger = Logger.getLogger(this.getClass());

    protected final AtomicInteger totalRequestSent;
    protected final AtomicInteger successfulRequestCount;
    protected final AtomicInteger failedRequestCount;
    protected final int maxRetries = 7;
    protected final long retryWaitTimeBaseMS = 100;

    private static final int skiDayLenInMin = 420;

    private final static CountDownLatch shouldStartPhaseTwo = new CountDownLatch(1);
    private final static CountDownLatch shouldStartPhaseThree = new CountDownLatch(1);

    private final MultiThreadedHttpConnectionManager connectionManager;
    protected final HttpClient client;
    private final ObjectMapper mapper;
    private final String serverAddr;
    private final String apiPath;
    private final int serverPort;

    // Fields with default values.
    private int maxThreadCount;
    private int skierCount;
    private int skiLiftCount;
    private int skiDayNum;
    private String resortName;

    private final int[] skiLifts;
    private final String targetUrl;

    public SkierClientBase(final String serverAddr,
                           final String apiPath,
                           int serverPort,
                           int maxThreadCount,
                           int skierCount,
                           int skiLiftCount,
                           int skiDayNum,
                           final String resortName) {
        this.connectionManager = new MultiThreadedHttpConnectionManager();
        this.client = new HttpClient(connectionManager);
        this.mapper = new ObjectMapper();
        this.serverAddr = serverAddr;
        this.apiPath = apiPath;
        this.serverPort = serverPort;

        // Set values if present.
        this.maxThreadCount = maxThreadCount;
        this.skierCount = skierCount;
        this.skiLiftCount = skiLiftCount;
        this.skiDayNum = skiDayNum;
        this.resortName = resortName;

        this.skiLifts = getLiftRides();
        this.targetUrl = this.serverAddr + ":" + this.serverPort + this.apiPath;

        this.totalRequestSent = new AtomicInteger();
        this.successfulRequestCount = new AtomicInteger();
        this.failedRequestCount = new AtomicInteger();

        // Overriding the default HTTP connection pool thresholds as they are too low:
        // https://hc.apache.org/httpclient-3.x/threading.html
        // Each thread will have a dedicated slot reserved in the pool (for threads across all phases).
        int desiredConcurrentConnectionsCount = this.maxThreadCount / 2 + this.maxThreadCount;
        HttpConnectionManagerParams connectionManagerParams = this.client.getHttpConnectionManager().getParams();
        connectionManagerParams.setMaxTotalConnections(desiredConcurrentConnectionsCount);
        connectionManagerParams.setDefaultMaxConnectionsPerHost(desiredConcurrentConnectionsCount);
        connectionManagerParams.setMaxConnectionsPerHost(this.client.getHostConfiguration(), desiredConcurrentConnectionsCount);
    }

    public boolean executeSingleGetRequest(String targetUrl) {
        HttpMethod httpGet = new GetMethod(targetUrl);
        int statusCode = -1;

        for (int i = 0; i < maxRetries; i++) {
            try {
                // Execute HTTP GET request.
                statusCode = this.client.executeMethod(httpGet);

                // Get HTTP response.
//                String responseBody = httpGet.getResponseBodyAsString();

                // Deal with the response.
//            System.out.printf("Retrieved response is: %s, with code %d, thread id is: %s\n\n",
//                    responseBody,
//                    statusCode,
//                    Thread.currentThread().getId());

                if (statusCode == 200 || statusCode == 204) {
                    updateStatCounts(statusCode, targetUrl);
                    httpGet.releaseConnection();

                    return true;
                } else {
                    System.out.printf("Got error response from server for GET request %s, will wait and retry.\n", targetUrl);
                    AWSUtil.sleepExponentially(i, this.retryWaitTimeBaseMS);
                }
            } catch (IOException e) {
                System.out.printf("Failed to send GET request to %s, with error: %s\n\n", targetUrl, e);
            }
        }

        updateStatCounts(statusCode, targetUrl);
        httpGet.releaseConnection();

        return false;
    }

    public boolean executeSinglePOSTRequest(String targetUrl, String bodyJsonStr) {
        PostMethod httpPost = new PostMethod(targetUrl);
        int statusCode = -1;

        for (int i = 0; i < maxRetries; i++) {
            try {
                // Set request body content.
                StringRequestEntity entity = new StringRequestEntity(bodyJsonStr, "application/json", "UTF-8");
                httpPost.setRequestEntity(entity);

                // Execute HTTP POST request.
                statusCode = this.client.executeMethod(httpPost);

                // Get HTTP response.
//                String responseBody = httpPost.getResponseBodyAsString();

                // Deal with the response.
//            System.out.printf("Retrieved response is: %s, with code %d, thread id is: %s\n\n",
//                    responseBody,
//                    statusCode,
//                    Thread.currentThread().getId());

                if (statusCode == 200 || statusCode == 204) {
                    updateStatCounts(statusCode, targetUrl);
                    httpPost.releaseConnection();

                    return true;
                } else {
                    System.out.printf("Got error response from server for POST request %s, will wait and retry.\n", targetUrl);
                    AWSUtil.sleepExponentially(i, this.retryWaitTimeBaseMS);
                }
            } catch (IOException e) {
                System.out.printf("Failed to send POST request to %s, with error: %s\n\n", targetUrl, e);
            }
        }

        updateStatCounts(statusCode, targetUrl);
        httpPost.releaseConnection();

        return false;
    }

    public void startLoadSimulation() throws JsonProcessingException, InterruptedException, ExecutionException {
        System.out.printf("Start skier client load simulator\n");

        long startTime = System.currentTimeMillis();

        Thread phaseOne = new Thread(() -> {
            try {
                executePhaseOne();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread phaseTwo = new Thread(() -> {
            try {
                executePhaseTwo();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread phaseThree = new Thread(() -> {
            try {
                executePhaseThree();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        phaseOne.start();
        phaseTwo.start();
        phaseThree.start();

        phaseOne.join();
        phaseTwo.join();
        phaseThree.join();

        long endTime = System.currentTimeMillis();

        // Output summaries.
        System.out.printf("\n[Execution Summary]\nTotal Requests: %d\nSuccessful Requests: %d\nFailed Requests: %d\nWall Time: %d seconds\nThroughput: %d rps\n",
                this.totalRequestSent.get(),
                this.successfulRequestCount.get(),
                this.failedRequestCount.get(),
                (endTime - startTime) / 1000,
                (this.totalRequestSent.get()) / ((endTime - startTime) / 1000));
    }

    private void executePhaseOne() throws JsonProcessingException, InterruptedException, ExecutionException {
        // Setup requirements in phase 1.
        int targetThreadCount = this.maxThreadCount / 4;
        int[][] skierSplits = getSkierIdsSplits(targetThreadCount);
        int[] timeSlides = getTimeSlides(1);

        executePhase(1, targetThreadCount, skierSplits, timeSlides, 1000, 5);
    }

    private void executePhaseTwo() throws InterruptedException, ExecutionException, JsonProcessingException {
        shouldStartPhaseTwo.await(1, TimeUnit.SECONDS);

        // Setup requirements in phase 2.
        int targetThreadCount = this.maxThreadCount;
        int[][] skierSplits = getSkierIdsSplits(targetThreadCount);
        int[] timeSlides = getTimeSlides(2);

        executePhase(2, targetThreadCount, skierSplits, timeSlides, 1000, 5);
    }

    private void executePhaseThree() throws InterruptedException, ExecutionException, JsonProcessingException {
        shouldStartPhaseThree.await(1, TimeUnit.SECONDS);

        // Setup requirements in phase 3.
        int targetThreadCount = this.maxThreadCount / 4;
        int[][] skierSplits = getSkierIdsSplits(targetThreadCount);
        int[] timeSlides = getTimeSlides(3);

        executePhase(3, targetThreadCount, skierSplits, timeSlides, 1000, 10);
    }

    private void executePhase(int phaseNum,
                              int targetThreadCount,
                              int[][] skierSplits,
                              int[] timeSlides,
                              int targetPOSTRequestCount,
                              int targetGETRequestCount) throws InterruptedException, ExecutionException, JsonProcessingException {
        System.out.printf("Executing phase %d\n", phaseNum);

        // Setup the ExecutorService for requests.
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
        List<Callable<Optional<TaskResponseStat>>> tasks = new ArrayList<>();

        // Sending the POST requests in batch toward the threads pool.
        for (int i = 0; i < targetThreadCount; i++) {
            if (isPostRequest) {
                // Prepare POST requests for current thread.
                String[] proposedPOSTRequestBody = preparePOSTRequestBody(targetRequestCount, skierSplits[i], timeSlides);

                // Append to the finalized tasks list.
                tasks.addAll(assembleTasksForPOSTRequests(this.targetUrl, proposedPOSTRequestBody));
            } else {
                // Prepare GET requests for current thread.
                String[] proposedGETRequests = prepareGETRequestURL(targetRequestCount, skierSplits[i]);

                // Append to the finalized tasks list.
                tasks.addAll(assembleTasksForGETRequests(proposedGETRequests));
            }
        }

        // Now execute all the tasks concurrently.
        List<Future<Optional<TaskResponseStat>>> responses = executorService.invokeAll(tasks);

        // Process the responses.
        if (isPhaseOne) {
            processResponsesInThread(responses, true, false, targetRequestCount, targetThreadCount);
        } else if (isPhaseTwo) {
            processResponsesInThread(responses, false, true, targetRequestCount, targetThreadCount);
        } else {
            processResponsesInThread(responses, false, false, targetRequestCount, targetThreadCount);
        }
    }

    private boolean sendSerialPOSTRequestsInThread(String targetUrl,
                                                   String[] requestBody) {
        boolean executionResult = true;

        // Execute all tasks within a single Thread.
        List<Callable<Optional<TaskResponseStat>>> tasks = new ArrayList<>();
        for (int i = 0; i < requestBody.length; i++) {
            executionResult &= executeSinglePOSTRequest(targetUrl, requestBody[i]);
        }

        return executionResult;
    }

    private boolean sendSerialGETRequestsInThread(String[] targetUrls) {
        boolean executionResult = true;

        // Execute all tasks within a single Thread.
        for (int i = 0; i < targetUrls.length; i++) {
            executionResult &= executeSingleGetRequest(targetUrls[i]);
        }

        return executionResult;
    }

    private List<Callable<Optional<TaskResponseStat>>> assembleTasksForPOSTRequests(String targetUrl,
                                                                                    String[] requestBody) {
        // Setup the tasks.
        List<Callable<Optional<TaskResponseStat>>> tasks = new ArrayList<>();
        tasks.add(() -> {
            long startTime = System.currentTimeMillis();
            boolean executionResult = sendSerialPOSTRequestsInThread(targetUrl, requestBody);
            long endTime = System.currentTimeMillis();

            return Optional.of(toTaskResponseStat(startTime, endTime, executionResult));
        });

        return tasks;
    }

    private List<Callable<Optional<TaskResponseStat>>> assembleTasksForGETRequests(String[] targetUrls) {
        // Setup the tasks.
        List<Callable<Optional<TaskResponseStat>>> tasks = new ArrayList<>();
        tasks.add(() -> {
            long startTime = System.currentTimeMillis();
            boolean executionResult = sendSerialGETRequestsInThread(targetUrls);
            long endTime = System.currentTimeMillis();

            return Optional.of(toTaskResponseStat(startTime, endTime, executionResult));
        });

        return tasks;
    }

    private void processResponsesInThread(List<Future<Optional<TaskResponseStat>>> responses,
                                          boolean isPhaseOne,
                                          boolean isPhaseTwo,
                                          int targetRequestCountPerThread,
                                          int targetThreadCount) throws ExecutionException, InterruptedException {
        // Waiting for all requests to be completed.
        while (!responses.stream().allMatch(stat -> stat.isDone())) {
            System.out.println("Waiting for all requests to complete...");

            // Check if at least 10% of the threads complete their works.
            List<Future<Optional<TaskResponseStat>>> completedRequests = responses.stream()
                    .filter(stat -> stat.isDone()).collect(Collectors.toList());

            // If so, execute the next phase.
            if (isPhaseOne || isPhaseTwo) {
                if (getCompletedThreadCount(completedRequests, targetRequestCountPerThread) > (targetThreadCount / 10)) {
                    if (isPhaseOne) {
                        System.out.println("Phase 1 reached at least 10% threads completion, notifying phase 2.");
                        shouldStartPhaseTwo.countDown();
                    } else if (isPhaseTwo) {
                        System.out.println("Phase 2 reached at least 10% threads completion, notifying phase 3.");
                        shouldStartPhaseThree.countDown();
                    }
                }
            }

            // Then wait for all requests to complete before further processing the responses.
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                System.out.printf("Encountered error during wait sleep %s\n", e);
            }
        }

        // All requests should now be completed.
        for (Future<Optional<TaskResponseStat>> response : responses) {
            try {
                Optional<TaskResponseStat> statHolder = response.get();
                if (statHolder.isPresent()) {
//                    TaskResponseStat stat = statHolder.get();
//                    long timeTaken = stat.getEndTime() - stat.getStartTime();
//                    System.out.printf("Response in thread %s took %d milliseconds in total, and has %s error\n",
//                            stat.getThreadId(),
//                            timeTaken,
//                            (stat.isEncounteredError()) ? "encountered" : "no");
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

    private TaskResponseStat toTaskResponseStat(long startTime, long endTime, boolean executionResult) {
        TaskResponseStat.TaskResponseStatBuilder statBuilder = TaskResponseStat.builder()
                .startTime(startTime)
                .endTime(endTime)
                .threadId(Thread.currentThread().getId());
        if (executionResult) {
            statBuilder.encounteredError(false);
        } else {
            statBuilder.encounteredError(true);
        }

        return statBuilder.build();
    }

    private int getCompletedThreadCount(List<Future<Optional<TaskResponseStat>>> completedTasks, int targetRequestCountPerThread) throws ExecutionException, InterruptedException {
        Map<Long, Integer> threadIdToCompletedRequestCount = new HashMap<>();

        for (Future<Optional<TaskResponseStat>> task : completedTasks) {
            if (task.get().isPresent()) {
                TaskResponseStat stat = task.get().get();

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

    private String[] preparePOSTRequestBody(int targetRequestCount,
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

            SkierPOSTRequest body = SkierPOSTRequest.builder()
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

    private String[] prepareGETRequestURL(int targetRequestCount,
                                          int[] skierSplit) {
        String[] requests = new String[targetRequestCount * 2];

        // Generate GET requests for https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/1.13#/skiers/getSkierDayVertical
        shuffleArray(skierSplit);
        int i = 0, j = 0;
        while (j < targetRequestCount) {
            if (i >= skierSplit.length) {
                i = 0;
            }

            StringBuilder targetUrl = new StringBuilder();
            targetUrl.append(this.serverAddr);
            targetUrl.append(":");
            targetUrl.append(this.serverPort);
            targetUrl.append("/IntelliJ_war/skiers/");
            targetUrl.append(preparePathParam(this.resortName));
            targetUrl.append("/days/");
            targetUrl.append(this.skiDayNum);
            targetUrl.append("/skiers/");
            targetUrl.append(skierSplit[i]);

            requests[j] = targetUrl.toString();

            i++;
            j++;
        }

        // Generate GET requests for https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/1.13#/skiers/getSkierResortTotals
        shuffleArray(skierSplit);
        i = 0;
        int k = 0;
        while (k < targetRequestCount) {
            if (i >= skierSplit.length) {
                i = 0;
            }

            StringBuilder targetUrl = new StringBuilder();
            targetUrl.append(this.serverAddr);
            targetUrl.append(":");
            targetUrl.append(this.serverPort);
            targetUrl.append("/IntelliJ_war/skiers/");
            targetUrl.append(skierSplit[i]);
            targetUrl.append("/vertical?resort=");
            targetUrl.append(preparePathParam(this.resortName));

            requests[j] = targetUrl.toString();

            i++;
            j++;
            k++;
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

    protected void updateStatCounts(int statusCode, String targetUrl) {
        this.totalRequestSent.addAndGet(1);

        if (statusCode == 200 || statusCode == 204) {
            this.successfulRequestCount.addAndGet(1);
        } else {
            this.failedRequestCount.addAndGet(1);
            this.logger.error("Detected failed request toward " + targetUrl);
        }
    }

    private String preparePathParam(String pathParam) {
        return pathParam.replaceAll(" ", "%20");
    }

//    private void sleepExponentially(int sleepTimes) {
//        try {
//            Thread.sleep(retryWaitTimeBaseMS * 2 * sleepTimes);
//        } catch (InterruptedException e) {
//            logger.error("Failed to sleep during retries attempt.", e);
//        }
//    }
}
