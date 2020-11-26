package listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import model.SkierPOSTRequest;
import org.apache.log4j.Logger;
import server.IkkyoneServlet;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sqs.model.Message;
import utility.AWSUtil;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IkkyoneQueueProcessor implements ServletContextListener {
    private final static Logger logger = Logger.getLogger(IkkyoneQueueProcessor.class);

    private final static ObjectMapper mapper = new ObjectMapper();
    private final static String itemAttrLiftTimes = "LiftTimes";
    private final static String itemAttrLiftIDs = "LiftIDs";
    private final static String itemAttrLiftDays = "LiftDays";
    private final static String itemAttrLastUpdateTime = "LastUpdateTime";
    private final static String itemTmpAttrOldUpdateTime = "OldUpdateTime";

    private final int maxRetries = 7;
    private final long retryWaitTimeBaseMS = 100;
    private final int targetPollerThreadCount = 60;

    private Thread processorThread = null;

    public void contextInitialized(ServletContextEvent sce) {
        if (processorThread == null || (!processorThread.isAlive())) {
            processorThread = new Thread(() -> {
                ExecutorService executorService = Executors.newFixedThreadPool(targetPollerThreadCount);

                for (int i = 0; i < targetPollerThreadCount; i++) {
                    executorService.execute(() -> {
                        while (true) {
                            try {
                                processMessagesInQueue();
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }

                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
            });

            processorThread.start();
        }
    }

    public void contextDestroyed(ServletContextEvent sce) {
        try {
            processorThread.interrupt();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processMessagesInQueue() throws JsonProcessingException {
        List<Message> messages = AWSUtil.getMsgFromSQS(IkkyoneServlet.sqsQueueName);

        for (Message message : messages) {
            SkierPOSTRequest request = mapper.readValue(message.body(), SkierPOSTRequest.class);

            if (writeToDDB(request)) {
                AWSUtil.deleteMsgInSQS(IkkyoneServlet.sqsQueueName, message.receiptHandle());
            }
        }
    }

    private boolean writeToDDB(@NonNull final SkierPOSTRequest skierRequest) {
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
                if (!AWSUtil.putItemDDB(IkkyoneServlet.ddbTableName,
                        item,
                        Optional.of(conditionExpression),
                        logger)) {
                    AWSUtil.sleepExponentially(i, this.retryWaitTimeBaseMS);
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

                if (!AWSUtil.updateItemDDB(IkkyoneServlet.ddbTableName,
                        IkkyoneServlet.itemPrimaryHashKey,
                        itemPrimaryHashKeyVal,
                        IkkyoneServlet.itemPrimarySortKey,
                        itemPrimarySortKeyVal,
                        updateExpression.substring(0, updateExpression.length() - 2),
                        Optional.of(conditionExpression),
                        attrNameAliases,
                        attrValueAliases,
                        logger)) {
                    AWSUtil.sleepExponentially(i, this.retryWaitTimeBaseMS);
                } else {
                    return true;
                }
            }
        }

        return false;
    }

    Map<String, AttributeValue> prepareDDBItem(@NonNull final String itemPrimaryHashKeyVal,
                                               @NonNull final String itemPrimarySortKeyVal,
                                               @NonNull final SkierPOSTRequest skierRequest,
                                               @NonNull final Long[] oldUpdateTimeHolder,
                                               @NonNull final Boolean[] itemExistCheckerHolder) {
        Map<String, AttributeValue> item = AWSUtil.getItemFromDB(IkkyoneServlet.ddbTableName,
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
}
