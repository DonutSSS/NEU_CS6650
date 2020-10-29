package server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import model.SkierGETRequest;
import model.SkierPOSTRequest;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import utility.AWSUtil;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IkkyoneServlet extends javax.servlet.http.HttpServlet {
    private final static Logger logger = Logger.getLogger(IkkyoneServlet.class);

    private final static String ddbTableName = "IkkyoneSkierTable";
    private final static String itemPrimaryHashKey = "SkierID";
    private final static String itemPrimarySortKey = "ResortID";
    private final static String itemAttrDailyTotalVerticals = "DailyTotalVerticals";
    private final static String itemAttrLiftTimes = "LiftTimes";
    private final static String itemAttrLiftIDs = "LiftIDs";
    private final static String itemAttrLiftDays = "LiftDays";
    private final static String itemAttrLastUpdateTime = "LastUpdateTime";
    private final static String itemTmpAttrOldUpdateTime = "OldUpdateTime";
    private final static ObjectMapper mapper = new ObjectMapper();

    private final int maxRetries = 7;
    private final long retryWaitTimeBaseMS = 100;

    protected void doPost(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws IOException {
        // Prepare response data.
        response.setContentType("text/plain");
        boolean processResult = false;
        JsonNode[] nodeHolder = new JsonNode[1];

        if (isValidRequest(request,
                response,
                false,
                new Integer[1],
                new SkierGETRequest[1])) {
            // Check POST body.
            if (isBodyValidOnPost(request, response, nodeHolder)) {
                // Get skier request body.
                SkierPOSTRequest skierRequest;
                try {
                    skierRequest = mapper.readValue(nodeHolder[0].toString(), SkierPOSTRequest.class);
                } catch (IOException e) {
                    logger.error("Failed to parse Skier post request body: ", e);
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    response.getWriter().printf("Server fails to parse request, with url: %s, and body: %s\n",
                            request.getPathInfo(),
                            nodeHolder[0].toPrettyString());
                    return;
                }

                final String itemPrimaryHashKeyVal = Integer.toString(skierRequest.getSkierID());
                final String itemPrimarySortKeyVal = skierRequest.getResortID();
                Long[] oldUpdateTimeHolder = new Long[1];
                Boolean[] itemExistCheckerHolder = new Boolean[1];

                // Perform exponential retries in case of race condition.
                for (int i = 0; i < maxRetries; i++) {
                    // Read from DDB to check if item exists.
                    Map<String, AttributeValue> item = prepareDDBItem(itemPrimaryHashKeyVal,
                            itemPrimarySortKeyVal,
                            skierRequest,
                            oldUpdateTimeHolder,
                            itemExistCheckerHolder);

                    // Update the item in DB accordingly.
                    if (itemExistCheckerHolder[0] == false) {       // Item doesn't exist.
                        // To prevent concurrent modification;
                        String conditionExpression = "attribute_not_exists(" + itemAttrLastUpdateTime + ")";

                        // Perform exponential backoff in retries.
                        if (!AWSUtil.putItemDDB(ddbTableName,
                                item,
                                Optional.of(conditionExpression),
                                logger)) {
                            AWSUtil.sleepExponentially(i, this.retryWaitTimeBaseMS);
                        } else {
                            processResult = true;
                            break;
                        }
                    } else {                                        // Item exists.
                        // Build update expression.
                        Map<String, String> attrNameAliases = new HashMap<>();
                        item.forEach((k, v) -> attrNameAliases.put("#" + k, k));
                        Map<String, AttributeValue> attrValueAliases = new HashMap<>();
                        item.forEach((k, v) -> attrValueAliases.put(":" + k, v));

                        StringBuilder builder = new StringBuilder();
                        builder.append("SET ");
                        item.forEach((k, v) -> builder.append("#" + k + " = :" + k + ", "));
                        String updateExpression = builder.toString();

                        // Build condition expression.
                        String conditionExpression = "#" + itemAttrLastUpdateTime
                                + " = :" + itemTmpAttrOldUpdateTime;
                        attrValueAliases.put(":" + itemTmpAttrOldUpdateTime,
                                AttributeValue.builder().n(Long.toString(oldUpdateTimeHolder[0])).build());

                        if (!AWSUtil.updateItemDDB(ddbTableName,
                                itemPrimaryHashKey,
                                itemPrimaryHashKeyVal,
                                itemPrimarySortKey,
                                itemPrimarySortKeyVal,
                                updateExpression.substring(0, updateExpression.length() - 2),
                                Optional.of(conditionExpression),
                                attrNameAliases,
                                attrValueAliases,
                                logger)) {
                            AWSUtil.sleepExponentially(i, this.retryWaitTimeBaseMS);
                        } else {
                            processResult = true;
                            break;
                        }
                    }
                }
            }
        }

        if (processResult) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().printf("It works! Post request url: %s, body: %s\n",
                    request.getPathInfo(),
                    nodeHolder[0].toPrettyString());
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().printf("Server fails to handle request, with url: %s, and body: %s\n",
                    request.getPathInfo(),
                    nodeHolder[0].toPrettyString());
        }
    }

    protected void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws IOException {
        // Prepare response data.
        response.setContentType("text/plain");

        Integer[] getRequestType = new Integer[1];
        SkierGETRequest[] getRequestHolder = new SkierGETRequest[1];
        Integer[] outcomeVerticalHolder = new Integer[1];
        boolean processResult = false;

        if (isValidRequest(request, response,
                true,
                getRequestType,
                getRequestHolder)) {
            if (getRequestType[0] == 1) {
                // API: https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/1.13#/resorts/getTopTenVert
                // TODO: Add logics for API: /resort/day/top10vert
                processResult = true;
            } else if (getRequestType[0] == 2) {
                // API: https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/1.13#/skiers/getSkierResortTotals
                getVerticalAtResort(getRequestHolder[0].skierID,
                        getRequestHolder[0].resortID,
                        outcomeVerticalHolder);
                processResult = true;
            } else if (getRequestType[0] == 3) {
                // API: https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/1.13#/skiers/getSkierDayVertical
                getVerticalAtResortAndDay(getRequestHolder[0].skierID,
                        getRequestHolder[0].resortID,
                        Integer.toString(getRequestHolder[0].dayID),
                        outcomeVerticalHolder);
                processResult = true;
            } else {
                throw new RuntimeException("Detected unsupport GET request.");
            }
        }

        if (processResult) {
            if (outcomeVerticalHolder[0] != null) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().printf("It works! GET request url: %s\nResult Vertical: %d\n",
                        request.getPathInfo(),
                        outcomeVerticalHolder[0]);
            } else {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().printf("It works! GET request url: %s, but no record is found\n",
                        request.getPathInfo());
            }
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().printf("Server fails to handle request, with url: %s\n",
                    request.getPathInfo());
        }
    }

    private void getVerticalAtResort(@NonNull final String itemPrimaryHashKeyVal,
                                        @NonNull final String itemPrimarySortKeyVal,
                                        @NonNull final Integer[] outcomeVerticalHolder) {
        Map<String, AttributeValue> item = AWSUtil.getItemFromDB(ddbTableName,
                itemPrimaryHashKey,
                itemPrimaryHashKeyVal,
                itemPrimarySortKey,
                itemPrimarySortKeyVal,
                logger);

        Map<String, AttributeValue> dailyVerticals = item.get(itemAttrDailyTotalVerticals).m();
        if (dailyVerticals != null && !dailyVerticals.isEmpty()) {
            final AtomicInteger totalVerticalsOnAllDays = new AtomicInteger();
            dailyVerticals.entrySet().stream().forEach(e -> totalVerticalsOnAllDays.addAndGet(Integer.parseInt(e.getValue().n())));

            int resultVertical = totalVerticalsOnAllDays.get();
            outcomeVerticalHolder[0] = resultVertical;
        }
    }

    private void getVerticalAtResortAndDay(@NonNull final String itemPrimaryHashKeyVal,
                                              @NonNull final String itemPrimarySortKeyVal,
                                              @NonNull final String dayID,
                                              @NonNull final Integer[] outcomeVerticalHolder) {
        Map<String, AttributeValue> item = AWSUtil.getItemFromDB(ddbTableName,
                itemPrimaryHashKey,
                itemPrimaryHashKeyVal,
                itemPrimarySortKey,
                itemPrimarySortKeyVal,
                logger);

        if (item != null && !item.isEmpty()) {
            Map<String, AttributeValue> dailyVerticals = item.get(itemAttrDailyTotalVerticals).m();
            if (dailyVerticals != null && dailyVerticals.get(dayID) != null) {
                outcomeVerticalHolder[0] = Integer.parseInt(dailyVerticals.get(dayID).n());
            }
        }
    }

    Map<String, AttributeValue> prepareDDBItem(@NonNull final String itemPrimaryHashKeyVal,
                                               @NonNull final String itemPrimarySortKeyVal,
                                               @NonNull final SkierPOSTRequest skierRequest,
                                               @NonNull final Long[] oldUpdateTimeHolder,
                                               @NonNull final Boolean[] itemExistCheckerHolder) {
        Map<String, AttributeValue> item = AWSUtil.getItemFromDB(ddbTableName,
                itemPrimaryHashKey,
                itemPrimaryHashKeyVal,
                itemPrimarySortKey,
                itemPrimarySortKeyVal,
                logger);

        Set<String> newLiftTimes = new HashSet<>();
        Set<String> newLiftIDs = new HashSet<>();
        Set<String> newLiftDays = new HashSet<>();
        Map<String, AttributeValue> dailyVerticals = new HashMap<>();
        String dayIDStr = Integer.toString(skierRequest.getDayID());
        Integer newlyCumulativeVertical = skierRequest.getLiftID() * 10;
        long newUpdateTime = System.currentTimeMillis();
        Map<String, AttributeValue> newItem = new HashMap<>();
        if (item != null && !item.isEmpty()) {     // Item exists.
            newItem = new HashMap<>(item);
            newItem.remove(itemPrimaryHashKey);
            newItem.remove(itemPrimarySortKey);

            newLiftTimes = new HashSet<>(item.get(itemAttrLiftTimes).ns());
            newLiftTimes.add(Integer.toString(skierRequest.getTime()));

            newLiftIDs = new HashSet<>(item.get(itemAttrLiftIDs).ns());
            newLiftIDs.add(Integer.toString(skierRequest.getLiftID()));

            newLiftDays = new HashSet<>(item.get(itemAttrLiftDays).ns());
            newLiftDays.add(dayIDStr);

            dailyVerticals = new HashMap<>(item.get(itemAttrDailyTotalVerticals).m());
            AttributeValue curDailyTotalVertical = dailyVerticals.get(dayIDStr);
            Integer newDailyTotalVertical;
            if (curDailyTotalVertical != null) {
                newDailyTotalVertical = Integer.parseInt(curDailyTotalVertical.n()) + newlyCumulativeVertical;
            } else {
                newDailyTotalVertical = newlyCumulativeVertical;
            }
            dailyVerticals.put(dayIDStr, AttributeValue.builder().n(Integer.toString(newDailyTotalVertical)).build());

            oldUpdateTimeHolder[0] = Long.parseLong(item.get(itemAttrLastUpdateTime).n());
            itemExistCheckerHolder[0] = true;
        } else {                                    // Item doesn't exist.
            newLiftTimes.add(Integer.toString(skierRequest.getTime()));
            newLiftIDs.add(Integer.toString(skierRequest.getLiftID()));
            newLiftDays.add(Integer.toString(skierRequest.getDayID()));

            dailyVerticals.put(dayIDStr, AttributeValue.builder().n(Integer.toString(newlyCumulativeVertical)).build());

            // Prepare key attributes only for DDB PutItem request.
            newItem.put(itemPrimaryHashKey, AttributeValue.builder().s(itemPrimaryHashKeyVal).build());
            newItem.put(itemPrimarySortKey, AttributeValue.builder().s(itemPrimarySortKeyVal).build());

            itemExistCheckerHolder[0] = false;
        }

        newItem.put(itemAttrLiftTimes, AttributeValue.builder().ns(newLiftTimes).build());
        newItem.put(itemAttrLiftIDs, AttributeValue.builder().ns(newLiftIDs).build());
        newItem.put(itemAttrLiftDays, AttributeValue.builder().ns(newLiftDays).build());
        newItem.put(itemAttrDailyTotalVerticals, AttributeValue.builder().m(dailyVerticals).build());
        newItem.put(itemAttrLastUpdateTime, AttributeValue.builder().n(Long.toString(newUpdateTime)).build());

        return newItem;
    }

//    private void sleepExponentially(int sleepTimes) {
//        try {
//            Thread.sleep(retryWaitTimeBaseMS * 2 * sleepTimes);
//        } catch (InterruptedException e) {
//            logger.error("Failed to sleepExponentially during retries attempt.", e);
//        }
//    }

    private boolean isValidRequest(javax.servlet.http.HttpServletRequest request,
                                   javax.servlet.http.HttpServletResponse response,
                                   boolean isGETRequest,
                                   Integer[] getRequestType,
                                   SkierGETRequest[] getRequestHolder) throws IOException {
        // Get URL Path from request.
        String urlPath = request.getPathInfo();

        // Check if URL Path exists.
        if (!hasPath(urlPath)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Missing path in url.");

            return false;
        }

        // Parse URL Path.
        String[] urlParts = urlPath.split("/");

        // Check if URL Path is valid.
        if (!hasValidPath(request.getQueryString(),
                urlParts,
                isGETRequest,
                getRequestType,
                getRequestHolder,
                response)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().printf("Unsupported path in url: %s | %s | %d\n", urlPath, Arrays.toString(urlParts), urlParts.length);

            return false;
        }

        return true;
    }

    private boolean hasPath(String urlPath) {
        return urlPath != null && urlPath.length() > 0;
    }

    // Based on the API spec in: https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/1.13#
    private boolean hasValidPath(String queryStr,
                                 String[] urlPath,
                                 boolean isGetRequest,
                                 Integer[] getRequestType,
                                 SkierGETRequest[] getRequestHolder,
                                 javax.servlet.http.HttpServletResponse response) throws IOException {
        // Validate the request url path according to the API spec
        // urlPath  = "/1/seasons/2019/day/1/skier/123"
        // urlParts = [, 1, seasons, 2019, day, 1, skier, 123]
        if (isGetRequest) {
            return isValidGETPath(queryStr,
                    urlPath,
                    getRequestType,
                    getRequestHolder,
                    response);
        } else {
            return isValidPOSTPath(urlPath);
        }
    }

    private boolean isValidGETPath(String queryStr,
                                   String[] urlPath,
                                   Integer[] getRequestType,
                                   SkierGETRequest[] getRequestHolder,
                                   javax.servlet.http.HttpServletResponse response) throws IOException {
        if (urlPath.length == 4) {
            if (urlPath[1].equals("resort") &&
                    urlPath[2].equals("day") &&
                    urlPath[3].equals("top10vert")) {
                getRequestType[0] = 1;

                final List<NameValuePair> params = URLEncodedUtils.parse(queryStr, StandardCharsets.UTF_8);
                SkierGETRequest request = new SkierGETRequest();

                for (final NameValuePair param : params) {
                    if (param.getName().equals("resort")) {
                        request.resortID = param.getValue();
                    } else if (param.getName().equals("dayID")) {
                        request.dayID = Integer.parseInt(param.getValue());
                        if (request.dayID < 1 || request.dayID > 366) {
                            return false;
                        }
                    } else {
                        logger.error("Detected unsupported query parameter type: " + param.getName() + ": " + param.getValue());
                        return false;
                    }
                }

                getRequestHolder[0] = request;

                return true;
            } else if (urlPath[1].equals("skiers") &&
                    !urlPath[2].isEmpty() &&
                    urlPath[3].equals("vertical")) {
                getRequestType[0] = 2;

                final List<NameValuePair> params = URLEncodedUtils.parse(queryStr, StandardCharsets.UTF_8);
                SkierGETRequest request = new SkierGETRequest();
                request.skierID = urlPath[2];

                for (final NameValuePair param : params) {
                    response.getWriter().println(param.getName() + ": " + param.getValue());
                    if (param.getName().equals("resort")) {
                        request.resortID = param.getValue();
                    } else {
                        logger.error("Detected unsupported query parameter type: " + param.getName() + ": " + param.getValue());
                        return false;
                    }
                }

                getRequestHolder[0] = request;

                return true;
            } else {
                logger.error("Detected unsupported url path: " + Arrays.toString(urlPath));
                return false;
            }
        } else if (urlPath.length == 7) {
            if (urlPath[1].equals("skiers") &&
                    !urlPath[2].isEmpty() &&
                    urlPath[3].equals("days") &&
                    !urlPath[4].isEmpty() &&
                    urlPath[5].equals("skiers") &&
                    !urlPath[6].isEmpty()) {
                getRequestType[0] = 3;

                SkierGETRequest request = new SkierGETRequest();
                request.resortID = cleanPathParam(urlPath[2]);
                request.dayID = Integer.parseInt(cleanPathParam(urlPath[4]));
                request.skierID = cleanPathParam(urlPath[6]);

                if (request.dayID < 1 || request.dayID > 366) {
                    return false;
                }

                getRequestHolder[0] = request;

                return true;
            } else {
                logger.error("Detected unsupported url path: " + Arrays.toString(urlPath));
                return false;
            }
        } else {
            logger.error("Detected unsupported url path: " + Arrays.toString(urlPath));
            return false;
        }
    }

    private boolean isValidPOSTPath(String[] urlPath) {
        if (urlPath.length == 3) {
            return urlPath[1].equals("skiers") &&
                    urlPath[2].equals("liftrides");
        }

        return false;
    }

    // Check if the string can be converted into a Json object, and retrieve that object for further processing.
    private boolean isBodyValidOnPost(javax.servlet.http.HttpServletRequest request,
                                      javax.servlet.http.HttpServletResponse response,
                                      JsonNode[] outputNodeHolder) throws IOException {
        String requestBody = getBodyStrOnPost(request.getReader());

        try {
            JsonNode node = mapper.readTree(requestBody);
            outputNodeHolder[0] = node;

            return true;
        } catch (IOException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Invalid body string in POST request.");

            return false;
        }
    }

    private String getBodyStrOnPost(BufferedReader buffIn) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;

        while((line = buffIn.readLine()) != null) {
            sb.append(line);
        }

        return sb.toString();
    }

    private String cleanPathParam(@NonNull final String pathParam) {
        return pathParam.replaceAll("%20", " ");
    }
}
