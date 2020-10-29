package clientAssignment1Part2;

import base.SkierClientBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import model.RequestResponseStat;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.kohsuke.args4j.CmdLineException;
import utility.AWSUtil;
import utility.SkierCmdLineHelper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;

public class SkierClientA1P2 extends SkierClientBase {
    private final String desiredOutputFilePath;
    private final Queue<RequestResponseStat> requestStats;

    public SkierClientA1P2(final String serverIp,
                           final String apiPath,
                           int serverPort,
                           int maxThreadCount,
                           int skierCount,
                           int skiLiftCount,
                           int skiDayNum,
                           String resortName,
                           final String desiredOutputFilePath) {
        super(serverIp,
                apiPath,
                serverPort,
                maxThreadCount,
                skierCount,
                skiLiftCount,
                skiDayNum,
                resortName);

        this.desiredOutputFilePath = desiredOutputFilePath;
        this.requestStats = new ConcurrentLinkedDeque<>();
    }

    @Override
    public boolean executeSingleGetRequest(String targetUrl) {
        HttpMethod httpGet = new GetMethod(targetUrl);
        int statusCode = -1;

        for (int i = 0; i < maxRetries; i++) {
            try {
                long startTime = System.currentTimeMillis();

                // Execute HTTP GET request.
                statusCode = this.client.executeMethod(httpGet);

                // Get request statistics.
                long latency = System.currentTimeMillis() - startTime;

                RequestResponseStat stat;
                if (targetUrl.contains("vertical")) {
                    stat = RequestResponseStat.builder()
                            .startTime(startTime)
                            .latency(latency)
                            .responseCode(statusCode)
                            .requestType("GET-SkierResortTotals")
                            .build();
                } else {
                    stat = RequestResponseStat.builder()
                            .startTime(startTime)
                            .latency(latency)
                            .responseCode(statusCode)
                            .requestType("GET-SkierDayVertical")
                            .build();
                }

                // Save request statistics.
                requestStats.offer(stat);

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
                    AWSUtil.sleepExponentially(i, retryWaitTimeBaseMS);
                }
            } catch (IOException e) {
                System.out.printf("Failed to send GET request to %s, with error: %s\n\n", targetUrl, e);
            }
        }

        updateStatCounts(statusCode, targetUrl);
        httpGet.releaseConnection();

        return false;
    }

    @Override
    public boolean executeSinglePOSTRequest(String targetUrl, String bodyJsonStr) {
        PostMethod httpPost = new PostMethod(targetUrl);
        int statusCode = -1;

        for (int i = 0; i < maxRetries; i++) {
            try {
                long startTime = System.currentTimeMillis();

                // Set request body content.
                StringRequestEntity entity = new StringRequestEntity(bodyJsonStr, "application/json", "UTF-8");
                httpPost.setRequestEntity(entity);

                // Execute HTTP POST request.
                statusCode = this.client.executeMethod(httpPost);

                // Get request statistics.
                long latency = System.currentTimeMillis() - startTime;
                RequestResponseStat stat = RequestResponseStat.builder()
                        .startTime(startTime)
                        .latency(latency)
                        .responseCode(statusCode)
                        .requestType("POST")
                        .build();

                // Save request statistics.
                requestStats.offer(stat);

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

    @Override
    public void startLoadSimulation() throws JsonProcessingException, InterruptedException, ExecutionException {
        super.startLoadSimulation();

        writeStats();

        calculateAndDisplayStats();
    }

    public void calculateAndDisplayStats() {
        System.out.printf("\n[Enhanced Statistics]:\n");

        // Calculate statistics for GETs.
        final List<Double> get1Stats = new ArrayList<>();
        this.requestStats.stream().filter(stat -> stat.getRequestType().equals("GET-SkierResortTotals"))
                .forEach(stat -> get1Stats.add(Double.parseDouble(Long.toString(stat.getLatency()))));

        DescriptiveStatistics get1DS = prepareStat(get1Stats);

        System.out.printf("Mean response time for all GETs-SkierResortTotals: %f ms\n", get1DS.getMean());
        System.out.printf("Median response time for all GETs-SkierResortTotals: %f ms\n", get1DS.getPercentile(50));
        System.out.printf("P99 response time for all GETs-SkierResortTotals: %f ms\n", get1DS.getPercentile(99));
        System.out.printf("Max response time for all GETs-SkierResortTotals: %f ms\n", get1DS.getMax());

        final List<Double> get2Stats = new ArrayList<>();
        this.requestStats.stream().filter(stat -> stat.getRequestType().equals("GET-SkierDayVertical"))
                .forEach(stat -> get2Stats.add(Double.parseDouble(Long.toString(stat.getLatency()))));

        DescriptiveStatistics get2DS = prepareStat(get2Stats);

        System.out.printf("Mean response time for all GETs-SkierDayVertical: %f ms\n", get2DS.getMean());
        System.out.printf("Median response time for all GETs-SkierDayVertical: %f ms\n", get2DS.getPercentile(50));
        System.out.printf("P99 response time for all GETs-SkierDayVertical: %f ms\n", get2DS.getPercentile(99));
        System.out.printf("Max response time for all GETs-SkierDayVertical: %f ms\n", get2DS.getMax());

        // Calculate statistics for POST.
        final List<Double> postStats = new ArrayList<>();
        this.requestStats.stream().filter(stat -> stat.getRequestType().equals("POST"))
                .forEach(stat -> postStats.add(Double.parseDouble(Long.toString(stat.getLatency()))));

        DescriptiveStatistics postDS = prepareStat(postStats);

        System.out.printf("Mean response time for all POSTs: %f ms\n", postDS.getMean());
        System.out.printf("Median response time for all POSTs: %f ms\n", postDS.getPercentile(50));
        System.out.printf("P99 response time for all POSTs: %f ms\n", postDS.getPercentile(99));
        System.out.printf("Max response time for all POSTs: %f ms\n", postDS.getMax());
    }

    private DescriptiveStatistics prepareStat(List<Double> stats) {
        DescriptiveStatistics statistics = new DescriptiveStatistics();

        stats.forEach(stat -> statistics.addValue(stat));

        return statistics;
    }

    // Write out stat records.
    private void writeStats() {
        File outputFile = new File(this.desiredOutputFilePath);

        CsvMapper mapper = new CsvMapper().configure(CsvGenerator.Feature.ALWAYS_QUOTE_EMPTY_STRINGS, true);
        CsvSchema schema = mapper.schemaFor(RequestResponseStat.class).withColumnSeparator(',');
        ObjectWriter writer = mapper.writer(schema);

        try (FileOutputStream fileOut = new FileOutputStream(outputFile);
             PrintWriter fileWriter = new PrintWriter(fileOut)) {
            writer.writeValue(fileWriter, this.requestStats);
        } catch (IOException e) {
            System.out.println("Failed to write statistics to output file: " + this.desiredOutputFilePath);
        }
    }

    public static void main(String[] args) throws JsonProcessingException, InterruptedException, ExecutionException, CmdLineException {
        SkierCmdLineHelper.CommandLineArgs parsedArgs = SkierCmdLineHelper.parseCommandLineArgs(args);

        SkierClientA1P2 client = new SkierClientA1P2(parsedArgs.serverAddr,
                parsedArgs.apiPath,
                parsedArgs.serverPort,
                parsedArgs.maxThreadCount,
                parsedArgs.skierCount,
                parsedArgs.skiLiftCount,
                parsedArgs.skiDay,
                parsedArgs.resortName,
                "testRecords.csv");

        client.startLoadSimulation();
    }
}
