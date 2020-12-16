package utility;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import model.SkierPOSTRequest;
import org.apache.log4j.Logger;
import server.IkkyoneServlet;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;

import static server.IkkyoneServlet.sqsQueueName;

public class AWSUtil {
    private final static Logger logger = Logger.getLogger(AWSUtil.class);

    private final static SdkHttpClient sdkHttpClient = ApacheHttpClient.builder()
            .maxConnections(800)
            .build();
    private final static DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .httpClient(sdkHttpClient)
            .build();
    private final static SqsClient sqsClient = SqsClient.builder()
            .httpClient(sdkHttpClient)
            .build();
    private static String queueUrl;

    private final static int maxRetries = 7;
    private final static long retryWaitTimeBaseMS = 100;
    private final static String itemAttrLiftTimes = "LiftTimes";
    private final static String itemAttrLiftIDs = "LiftIDs";
    private final static String itemAttrLiftDays = "LiftDays";
    private final static String itemAttrLastUpdateTime = "LastUpdateTime";
    private final static String itemTmpAttrOldUpdateTime = "OldUpdateTime";
    private final static ObjectMapper mapper = new ObjectMapper();

    public static boolean putItemDDB(@NonNull final String ddbTableName,
                                     @NonNull final Map<String, AttributeValue> item,
                                     @NonNull final Optional<String> conditionExpression,
                                     Logger logger) {
        PutItemRequest.Builder builder = PutItemRequest.builder()
                .tableName(ddbTableName)
                .item(item);

        if (conditionExpression.isPresent()) {
            builder.conditionExpression(conditionExpression.get());
        }

        PutItemRequest request = builder.build();
        try {
            dynamoDbClient.putItem(request);
        } catch (ResourceNotFoundException e) {
            logger.error("Failed to find DDB Table - " + ddbTableName, e);
            return false;
        } catch (DynamoDbException e) {
            logger.error("Failed to put item into DDB Table - " + ddbTableName, e);
            return false;
        }

        return true;
    }

    public static boolean updateItemDDB(@NonNull final String ddbTableName,
                                        @NonNull final String itemPrimaryHashKey,
                                        @NonNull final String itemPrimaryHashKeyVal,
                                        @NonNull final String itemPrimarySortKey,
                                        @NonNull final String itemPrimarySortKeyVal,
                                        @NonNull final String updateExpression,
                                        @NonNull final Optional<String> conditionExpression,
                                        @NonNull final Map<String, String> attrNameAliases,
                                        @NonNull final Map<String, AttributeValue> attrValueAliases,
                                        Logger logger) {
        Map<String, AttributeValue> itemKey = new HashMap<>();
        itemKey.put(itemPrimaryHashKey, AttributeValue.builder().s(itemPrimaryHashKeyVal).build());
        itemKey.put(itemPrimarySortKey, AttributeValue.builder().s(itemPrimarySortKeyVal).build());

        UpdateItemRequest.Builder builder = UpdateItemRequest.builder()
                .tableName(ddbTableName)
                .key(itemKey)
                .updateExpression(updateExpression)
                .expressionAttributeNames(attrNameAliases)
                .expressionAttributeValues(attrValueAliases);

        if (conditionExpression.isPresent()) {
            builder.conditionExpression(conditionExpression.get());
        }

        UpdateItemRequest request = builder.build();
        try {
            dynamoDbClient.updateItem(request);
        } catch (ResourceNotFoundException e) {
            logger.error("Failed to find DDB Table - " + ddbTableName, e);
            return false;
        } catch (DynamoDbException e) {
            logger.error("Failed to update item in DDB Table - " + ddbTableName, e);
            return false;
        }

        return true;
    }

    public static Map<String, AttributeValue> getItemFromDB(@NonNull final String ddbTableName,
                                                            @NonNull final String itemPrimaryHashKey,
                                                            @NonNull final String itemPrimaryHashKeyVal,
                                                            @NonNull final String itemPrimarySortKey,
                                                            @NonNull final String itemPrimarySortKeyVal,
                                                            Logger logger) {
        Map<String, AttributeValue> itemKey = new HashMap<>();
        itemKey.put(itemPrimaryHashKey, AttributeValue.builder().s(itemPrimaryHashKeyVal).build());
        itemKey.put(itemPrimarySortKey, AttributeValue.builder().s(itemPrimarySortKeyVal).build());

        GetItemRequest request = GetItemRequest.builder()
                .tableName(ddbTableName)
                .key(itemKey)
                .build();

        try {
            return dynamoDbClient.getItem(request).item();
        } catch (DynamoDbException e) {
            logger.error("Failed to get item from DDB. Key: " + itemPrimaryHashKey + " | keyVal: " + itemPrimaryHashKeyVal, e);
        }

        return null;
    }

    public static void sendMsgToSQS(@NonNull final String queueName,
                                    @NonNull final String messageBody) {
        if (queueUrl == null) {
            queueUrl = getQueueUrl(queueName);
        }

        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
//                .messageGroupId("group1")
                .build();

        try {
            sqsClient.sendMessage(request);
        } catch (SqsException e) {
            throw new RuntimeException(String.format("Failed to send message %s to queue %s",
                    messageBody,
                    queueName), e);
        }
    }

    public static List<Message> getMsgFromSQS(@NonNull final String queueName) {
        if (queueUrl == null) {
            queueUrl = getQueueUrl(queueName);
        }

        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();

        try {
            return sqsClient.receiveMessage(request).messages();
        } catch (SqsException e) {
            throw new RuntimeException(String.format("Failed to get messages from queue %s",
                    queueName));
        }
    }

    public static void deleteMsgInSQS(@NonNull final String queueName,
                                      @NonNull final String msgReceiptHandle) {
        if (queueUrl == null) {
            queueUrl = getQueueUrl(queueName);
        }

        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(msgReceiptHandle)
                .build();

        try {
            sqsClient.deleteMessage(request);
        } catch (SqsException e ) {
            throw new RuntimeException(String.format("Failed to delete message %s from queue %s",
                    msgReceiptHandle,
                    queueName));
        }
    }

    public static void sleepExponentially(int sleepTimes, long retryWaitTimeBaseMS) {
        try {
            long sleepTime = Math.max(1, retryWaitTimeBaseMS * 2 * sleepTimes);
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            return;
        }
    }

    public boolean publishToSQS(@NonNull final SkierPOSTRequest skierRequest) {
        try {
            String sqsMessageBody = mapper.writeValueAsString(skierRequest);

            sendMsgToSQS(sqsQueueName, sqsMessageBody);

            return true;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(String.format("Failed to write POST request to sqs str: %s", skierRequest));
        }
    }

    public static boolean writeToDDB(@NonNull final SkierPOSTRequest skierRequest) {
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
                if (!putItemDDB(IkkyoneServlet.ddbTableName,
                        item,
                        Optional.of(conditionExpression),
                        logger)) {
                    sleepExponentially(i, retryWaitTimeBaseMS);
                } else {
                    return true;
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

                if (!updateItemDDB(IkkyoneServlet.ddbTableName,
                        IkkyoneServlet.itemPrimaryHashKey,
                        itemPrimaryHashKeyVal,
                        IkkyoneServlet.itemPrimarySortKey,
                        itemPrimarySortKeyVal,
                        updateExpression.substring(0, updateExpression.length() - 2),
                        Optional.of(conditionExpression),
                        attrNameAliases,
                        attrValueAliases,
                        logger)) {
                    sleepExponentially(i, retryWaitTimeBaseMS);
                } else {
                    return true;
                }
            }
        }

        return false;
    }

    private static Map<String, AttributeValue> prepareDDBItem(@NonNull final String itemPrimaryHashKeyVal,
                                                              @NonNull final String itemPrimarySortKeyVal,
                                                              @NonNull final SkierPOSTRequest skierRequest,
                                                              @NonNull final Long[] oldUpdateTimeHolder,
                                                              @NonNull final Boolean[] itemExistCheckerHolder) {
        Map<String, AttributeValue> item = getItemFromDB(IkkyoneServlet.ddbTableName,
                IkkyoneServlet.itemPrimaryHashKey,
                itemPrimaryHashKeyVal,
                IkkyoneServlet.itemPrimarySortKey,
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
            newItem.remove(IkkyoneServlet.itemPrimaryHashKey);
            newItem.remove(IkkyoneServlet.itemPrimarySortKey);

            newLiftTimes = new HashSet<>(item.get(itemAttrLiftTimes).ns());
            newLiftTimes.add(Integer.toString(skierRequest.getTime()));

            newLiftIDs = new HashSet<>(item.get(itemAttrLiftIDs).ns());
            newLiftIDs.add(Integer.toString(skierRequest.getLiftID()));

            newLiftDays = new HashSet<>(item.get(itemAttrLiftDays).ns());
            newLiftDays.add(dayIDStr);

            dailyVerticals = new HashMap<>(item.get(IkkyoneServlet.itemAttrDailyTotalVerticals).m());
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
            newItem.put(IkkyoneServlet.itemPrimaryHashKey, AttributeValue.builder().s(itemPrimaryHashKeyVal).build());
            newItem.put(IkkyoneServlet.itemPrimarySortKey, AttributeValue.builder().s(itemPrimarySortKeyVal).build());

            itemExistCheckerHolder[0] = false;
        }

        newItem.put(itemAttrLiftTimes, AttributeValue.builder().ns(newLiftTimes).build());
        newItem.put(itemAttrLiftIDs, AttributeValue.builder().ns(newLiftIDs).build());
        newItem.put(itemAttrLiftDays, AttributeValue.builder().ns(newLiftDays).build());
        newItem.put(IkkyoneServlet.itemAttrDailyTotalVerticals, AttributeValue.builder().m(dailyVerticals).build());
        newItem.put(itemAttrLastUpdateTime, AttributeValue.builder().n(Long.toString(newUpdateTime)).build());

        return newItem;
    }

    private static String getQueueUrl(@NonNull final String queueName) {
        GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        GetQueueUrlResponse response = sqsClient.getQueueUrl(request);

        if (response.queueUrl() != null) {
            return response.queueUrl();
        }

        throw new IllegalArgumentException(String.format("Failed to retrieve queue url for queue: %s", queueName));
    }
}
