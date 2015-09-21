package co.antena.sqs.async.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import co.antena.sqs.async.SQSMessageHandler;
import co.antena.sqs.async.SQSProvider;
import co.antena.sqs.async.SQSQueuePoller;

import com.amazonaws.services.sqs.model.Message;

@EnableScheduling
public class SQSQueuePollerMultipleMessageImpl implements SQSQueuePoller {

    private static Logger logger = Logger.getLogger(SQSQueuePollerMultipleMessageImpl.class.getCanonicalName());

    private List<SQSMessageHandler> subscribers = new ArrayList<SQSMessageHandler>();
    private int qMessages = 1;

    private SQSProvider sqsProvider;
    private String queueUrl;
    private SQSMessageHandler handler;

    public SQSMessageHandler getHandler() {
        return handler;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public SQSProvider getSqsProvider() {
        return sqsProvider;
    }

    @PostConstruct
    public void initIt() throws Exception {
        subscribers.add(handler);
    }

    public void setHandler(SQSMessageHandler handler) {
        this.handler = handler;
    }

    public void setQueueUrl(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    public void setSqsProvider(SQSProvider sqsProvider) {
        this.sqsProvider = sqsProvider;
    }

    /**
     * Starts polling If the SQSMessageHandler returns true, then the message is
     * marked for delete. But if any of the SQSMessageHandler s returns false
     * then the message is not deleted.
     */
    @Override
    @Scheduled(initialDelay = 1000, fixedRate = 5000)
    public void start() {

        logger.info("Starting SQSQueuePoller");

        long time, recvTime, procTime, delTime;

        List<Message> messages;
        List<Message> toDeleteMessages;

        time = System.currentTimeMillis();
        messages = sqsProvider.receiveMessage(queueUrl, qMessages);

        if (messages.isEmpty()) {
            logger.info("Message was empty... long polling");
            messages = sqsProvider.receiveLPMessage(queueUrl, qMessages);
            logger.info("end long polling");
        }

        recvTime = System.currentTimeMillis();

        toDeleteMessages = new ArrayList<Message>(messages.size());
        for (Message message : messages) {
            logger.info("Message received");

            for (SQSMessageHandler messageHandler : subscribers) {
                logger.info("Passing to subscriber : " + messageHandler.toString());
                if (!messageHandler.handleMessage(message)) {
                    logger.warn("Message delete prevented by : " + messageHandler.toString());
                } else {
                    toDeleteMessages.add(message);
                }
            }
        }

        procTime = System.currentTimeMillis();

        logger.info("Deletting messages : " + toDeleteMessages.size());
        if (!toDeleteMessages.isEmpty()) {
            sqsProvider.deleteMessageBatch(toDeleteMessages, queueUrl);
        }

        delTime = System.currentTimeMillis();

        for (Message msg : toDeleteMessages) {
            logger.info("TIME FOR MESSAGEID : " + msg
                    .getMessageId() + " - RECEIVETIME : " + (recvTime - time) + " - PROCTIME : " + (procTime -
                    recvTime) + " - DELETETIME : " + (delTime - procTime));
        }

    }

    /**
     * Subscribe a MessageHandler for the Poller
     */
    @Override
    public void subscribe(SQSMessageHandler sqsMessageHandler) {
        logger.info("Adding new subscriber : " + sqsMessageHandler);
        subscribers.add(sqsMessageHandler);
    }

}
