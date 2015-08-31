package co.antena.sqs.async.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import co.antena.sqs.async.SQSMessageHandler;
import co.antena.sqs.async.SQSProvider;
import co.antena.sqs.async.SQSQueuePoller;

import com.amazonaws.services.sqs.model.Message;

@Service
public class SQSQueuePollerMultipleMessageImpl implements SQSQueuePoller {

	private static Logger logger = Logger.getLogger(SQSQueuePollerMultipleMessageImpl.class.getCanonicalName());

	private List<SQSMessageHandler> subscribers = new ArrayList<SQSMessageHandler>();
	@Autowired
	private SQSProvider sqsProvider;
	private int qMessages = 1;
	@Value("${queue.url}")
	private String queueUrl;

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
			logger.info("Messagge was empty... long polling");
			messages = sqsProvider.receiveLPMessage(queueUrl, qMessages);
			logger.info("end long polling");
		}

		recvTime = System.currentTimeMillis();

		toDeleteMessages = new ArrayList<Message>(messages.size());
		for (Message message : messages) {
			logger.info("Messagge received");

			for (SQSMessageHandler messageHandler : subscribers) {
				logger.info("Passing to suscriber : " + messageHandler.toString());
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
			logger.info("TIME FOR MESSAGEID : " + msg.getMessageId() + " - RECEIVETIME : " + (recvTime - time)
					+ " - PROCTIME : " + (procTime - recvTime) + " - DELETETIME : " + (delTime - procTime));
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
