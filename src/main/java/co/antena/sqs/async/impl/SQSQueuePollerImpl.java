package co.antena.sqs.async.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import co.antena.sqs.async.SQSMessageHandler;
import co.antena.sqs.async.SQSProvider;
import co.antena.sqs.async.SQSQueuePoller;

import com.amazonaws.services.sqs.model.Message;

/**
 * This class allows to poll a queue
 *
 * @author gonzalo
 */
// @Service
public class SQSQueuePollerImpl implements SQSQueuePoller {

	private static Logger logger = Logger.getLogger(SQSQueuePollerImpl.class.getCanonicalName());

	private List<SQSMessageHandler> subscribers = new ArrayList<SQSMessageHandler>();
	@Autowired
	private SQSProvider sqsProvider;
	@Value("${queue.url}")
	private String queueUrl;

	@Autowired
	private SQSMessageHandler handler;

	// @PostConstruct
	// public void initIt() throws Exception {
	// subscribers.add(handler);
	// }

	/**
	 * Starts polling If the SQSMessageHandler returns true, then the message is
	 * marked for delete. But if any of the SQSMessageHandler s returns false
	 * then the message is not deleted.
	 */
	@Override
	@Scheduled(initialDelay = 1000, fixedRate = 5000)
	public void start() {

		logger.info("Starting SQSQueuePoller");

		List<Message> messages;

		messages = sqsProvider.receiveMessage(queueUrl);

		if (messages.isEmpty()) {
			logger.info("Messagge was empty... long polling");
			messages = sqsProvider.receiveLPMessage(queueUrl);
			logger.info("end long polling");
		}

		boolean delete;
		for (Message message : messages) {
			logger.info("Messagge received");
			delete = true;
			for (SQSMessageHandler messageHandler : subscribers) {
				logger.info("Passing to suscriber : " + messageHandler.toString());
				if (!messageHandler.handleMessage(message)) {
					delete = false;
					logger.warn("Message delete prevented by : " + messageHandler.toString());
				}
			}
			if (delete) {
				logger.info("Deletting message : " + message.toString());
				sqsProvider.deleteMessage(message, queueUrl);
			} else {
				logger.warn("Exception processing message, NOT deleting: " + message.toString());
			}

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
