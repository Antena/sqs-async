package co.antena.sqs.async.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import co.antena.sqs.async.SQSMessageHandler;
import co.antena.sqs.async.SQSMessageHandlerException;
import co.antena.sqs.async.SQSProvider;
import co.antena.sqs.async.SQSQueuePoller;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;




/**
 * This class allows to poll a queue
 * @author gonzalo
 *
 */

public class SQSQueuePollerImpl implements SQSQueuePoller {

	private static Logger  logger = Logger.getLogger("co.antena.rossirix.batch.async.impl.SQSQueuePollerImpl");
	
	private List<SQSMessageHandler> subscribers = new ArrayList<SQSMessageHandler>();
	private SQSProvider sqsProvider;
	private String queueUrl;
	
	
	//Construct
	public SQSQueuePollerImpl(String queueUrl, SQSProvider sqsProvider){
		 this.queueUrl = queueUrl;
		 this.sqsProvider = sqsProvider;
	}
	
	
	/**
	 * Start polling
	 */
	@Override
	public void start() {

		logger.info("Starting SQSQueuePoller");
		
		List<Message> messages;
		while(true){
	
			messages = this.sqsProvider.receiveMessage(this.queueUrl);

			if(messages.isEmpty()){
				logger.info("Messagge was empty... long polling");
				messages = this.sqsProvider.receiveLPMessage(this.queueUrl);
				logger.info("end long polling");
			}
			
			boolean exception;
			for (Message message : messages) {
				logger.info("Messagge received");
				exception = false;
				for(SQSMessageHandler messageHandler : this.subscribers){
					logger.info("Passing to suscriber : " + messageHandler.toString());
					try {
						messageHandler.handleMessage(message);
					} catch (SQSMessageHandlerException e) {
						logger.error("Error processsing message", e);
						exception = true;
					}
				}
				if(!exception){
					logger.info("Deletting message : " + message.toString());
					this.sqsProvider.deleteMessage(message, queueUrl);					
				} else {
					logger.warn("Exception processing message, NOT deleting: " + message.toString());
				}
			}
			
		}
		

	}

	/**
	 * Subscribe a MessageHandler for the Poller
	 */
	@Override
	public void subscribe(SQSMessageHandler sqsMessageHandler) {
		logger.info("Adding new subscriber : " + sqsMessageHandler);
		this.subscribers.add(sqsMessageHandler);
	}

}
