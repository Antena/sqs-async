package co.antena.sqs.async.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;







import co.antena.sqs.async.SQSProvider;
import co.antena.sqs.async.SQSQueuePoller;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;


/**
 * Uses an actual AmazonSQS SDK client to perform the SQSProvider operations
 * @author gonzalo
 *
 */
public class SQSProviderImpl implements SQSProvider{

	private static Logger  logger = Logger.getLogger(SQSProviderImpl.class.getCanonicalName());
	
	private AmazonSQS sqs;
	

	public SQSProviderImpl(AmazonSQS sqs){
		this.sqs = sqs;
	}
	
	public SQSQueuePoller getPoller(String queueUrl){
		return new SQSQueuePollerImpl(queueUrl, this);
	}

	@Override
	public List<Message> receiveMessage(String queueName) {
		logger.debug("Getting message for queue : " +  queueName);
		List<Message> ret = new ArrayList<Message>();
		
		try{
			this.sqs.receiveMessage(queueName).getMessages();
		}catch(Exception e){
			logger.error("Error receiving message", e);
		}
		
		return ret;
	}
	
	@Override
	public List<Message> receiveMessage(String url, int qMessages) {

		logger.debug("Receiving long polling queue : " + url);
		
		ReceiveMessageRequest rmr = new ReceiveMessageRequest(url);
		rmr.setMaxNumberOfMessages(qMessages);
		
		List<Message> ret = new ArrayList<Message>();
		try{
			ret = this.sqs.receiveMessage(rmr).getMessages();
		}catch(Exception e){
			logger.error("Exception while receiving message", e);
		}
		
		logger.debug("Return receive long polling queue : " + url + " - qmessages : " + ret.size());
		
		return ret;
	}

	@Override
	public void deleteMessage(Message message, String queueUrl) {
		logger.debug("Deleting message : " + message.getBody() + " for queue : " +  queueUrl);
		
		try{
			sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
		}catch(Exception e){
			logger.error("Exception while deleting message", e);
		}
		
	}
	
	@Override
	public void deleteMessageBatch(List<Message> messages, String queueUrl) {
		logger.debug("Deleting messages batch : " + messages.size() + " for queue : " +  queueUrl);

		List<DeleteMessageBatchRequestEntry> entries = new ArrayList<DeleteMessageBatchRequestEntry>(messages.size());
		for(Message msg : messages){
			entries.add(new DeleteMessageBatchRequestEntry(msg.getMessageId(), msg.getReceiptHandle()));
		}
		
		DeleteMessageBatchRequest dmbr = new DeleteMessageBatchRequest(queueUrl, entries);
		
		try {
			sqs.deleteMessageBatch(dmbr);
		}catch(Exception e){
			logger.error("Exception while deleting messages batch", e);
		}
		
	}

	@Override
	public String createQueue(String queueName) {
		logger.debug("Creating queue : " +  queueName);
		String ret = null;

		try {
			ret = this.sqs.createQueue(queueName).getQueueUrl();
		}catch(Exception e){
			logger.error("Exception while creating queue", e);
		}
		
		return ret;
	}

	@Override
	public List<String> listQueues(String prefix) {
		logger.debug("Listing queues with prefix : " + prefix);

		List<String> ret = null;
		try{
			ret = this.sqs.listQueues(prefix).getQueueUrls();
		}catch(Exception e){
			logger.error("Exception while listing queues with prefix", e);
		}
		
		return ret;
	}

	@Override
	public List<String> listQueues() {
		logger.debug("Listing all queues");

		List<String> ret = null;
		try{
			this.sqs.listQueues().getQueueUrls();
		}catch(Exception e){
			logger.error("Exception while listing queues", e);
		}
		
		return ret;
	}

	@Override
	public String sendMessage(String url, String body) {
		logger.debug("Sending message : " + body + " to queue : " + url);
	
		String ret = null;
		try { 
			ret = sqs.sendMessage(url, body).getMessageId();
		}catch(Exception e){
			logger.error("Exception while sendind message", e);
		}

		return ret;
	}

	@Override
	public void deleteQueue(String url) {
		logger.debug("Deleting queue : " + url);
		try{
			this.sqs.deleteQueue(url);
		}catch(Exception e){
			logger.error("Exception while deleting queue", e);
		}
		
	}

	@Override
	public List<Message> receiveLPMessage(String url) {
		
		logger.debug("Receiving long polling queue : " + url);
		
		ReceiveMessageRequest rmr = new ReceiveMessageRequest(url);
		rmr.setWaitTimeSeconds(20);
		
		List<Message> ret = new ArrayList<Message>();
		try{
			ret = this.sqs.receiveMessage(rmr).getMessages();
		}catch(Exception e){
			logger.error("Exception while receiving message", e);
		}
		
		logger.debug("Return receive long polling queue : " + url + " - qmessages : " + ret.size());
		
		return ret;

	}
	
	@Override
	public List<Message> receiveLPMessage(String url, int qMessages) {
		
		logger.debug("Receiving long polling queue : " + url);
		
		ReceiveMessageRequest rmr = new ReceiveMessageRequest(url);
		rmr.setMaxNumberOfMessages(qMessages);
		rmr.setWaitTimeSeconds(20);
		
		List<Message> ret = new ArrayList<Message>();
		try{
			ret = this.sqs.receiveMessage(rmr).getMessages();
		}catch(Exception e){
			logger.error("Exception while receiving message", e);
		}
		
		logger.debug("Return receive long polling queue : " + url + " - qmessages : " + ret.size());
		
		return ret;

	}
}
