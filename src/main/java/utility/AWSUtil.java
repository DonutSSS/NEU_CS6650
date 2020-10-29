package utility;

import lombok.NonNull;
import org.apache.log4j.Logger;
import server.IkkyoneServlet;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AWSUtil {
//    private final static Logger logger = Logger.getLogger(IkkyoneServlet.class);

    private final static SdkHttpClient sdkHttpClient = ApacheHttpClient.builder()
            .maxConnections(800)
            .build();
    private final static DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .httpClient(sdkHttpClient)
            .build();

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

    public static void sleepExponentially(int sleepTimes, long retryWaitTimeBaseMS) {
        try {
            Thread.sleep(retryWaitTimeBaseMS * 2 * sleepTimes);
        } catch (InterruptedException e) {
            return;
        }
    }
}
