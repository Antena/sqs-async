package co.antena.sqs.async.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.sqs.model.Message;

import co.antena.sqs.async.SQSMessageHandler;
import co.antena.sqs.async.SQSProvider;
import co.antena.sqs.async.SQSQueuePoller;

public class SQSQueuePollerMultipleMessageImpl implements SQSQueuePoller {

	private static Logger  logger = Logger.getLogger(SQSQueuePollerMultipleMessageImpl.class.getCanonicalName());
	
	private List<SQSMessageHandler> subscribers = new ArrayList<SQSMessageHandler>();
	private SQSProvider sqsProvider;
	private int qMessages = 1;
	private String queueUrl;
	
	
	
	//Construct
	public SQSQueuePollerMultipleMessageImpl(String queueUrl, SQSProvider sqsProvider, int qMessages){
	    this.queueUrl = queueUrl;
	    this.sqsProvider = sqsProvider;
		this.qMessages = qMessages;
	}

	
	/**
	 * Starts polling
	 * If the SQSMessageHandler returns true, then the message is marked for delete.
	 * But if any of the SQSMessageHandler s returns false then the message is not deleted.
	 */
	@Override
	public void start() {

		logger.info("Starting SQSQueuePoller");

		long time, recvTime, procTime, delTime;
		
		List<Message> messages;
		List<Message> toDeleteMessages;
		while(true){
	
			time = System.currentTimeMillis();
			messages = this.sqsProvider.receiveMessage(this.queueUrl, this.qMessages);
			
			if(messages.isEmpty()){
				logger.info("Messagge was empty... long polling");
				messages = this.sqsProvider.receiveLPMessage(this.queueUrl, this.qMessages);
				logger.info("end long polling");
			}
			recvTime = System.currentTimeMillis();			
			
			toDeleteMessages = new ArrayList<Message>(messages.size());
			for (Message message : messages) {
				logger.info("Messagge received");

				for(SQSMessageHandler messageHandler : this.subscribers){
					logger.info("Passing to suscriber : " + messageHandler.toString());
					if(!messageHandler.handleMessage(message)){
						logger.warn("Message delete prevented by : " + messageHandler.toString());
					} else {
						toDeleteMessages.add(message);
					}
				}
			}
			
			procTime = System.currentTimeMillis();
			
			logger.info("Deletting messages : " + toDeleteMessages.size());
			if(!toDeleteMessages.isEmpty()){
				this.sqsProvider.deleteMessageBatch(toDeleteMessages, queueUrl);					
			} 
			
			delTime = System.currentTimeMillis();
			
			for(Message msg : toDeleteMessages){
				logger.info("TIME FOR MESSAGEID : " + msg.getMessageId() + " - RECEIVETIME : " + (recvTime - time) + " - PROCTIME : " + (procTime - recvTime) + " - DELETETIME : " + (delTime - procTime) );
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
