package listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.SkierPOSTRequest;
import server.IkkyoneServlet;
import software.amazon.awssdk.services.sqs.model.Message;
import utility.AWSUtil;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static utility.AWSUtil.writeToDDB;

public class IkkyoneQueueProcessor implements ServletContextListener {
    private final static ObjectMapper mapper = new ObjectMapper();

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
}
