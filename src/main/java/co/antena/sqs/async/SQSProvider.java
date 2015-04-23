package co.antena.sqs.async;

import java.util.List;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

/**
 * Abstracts an Amazon SQS SDK implementation
 * @author gonzalo
 *
 */
public interface SQSProvider {

	/**
	 * Receives a message
	 * @param receiveMessageRequest
	 * @return
	 */
	public List<Message> receiveMessage(String receiveMessageRequest);

	/**
	 * Receives a message with long polling
	 * @param receiveMessageRequest
	 * @return
	 */
	public List<Message> receiveLPMessage(String receiveMessageRequest);
	
	/**
	 * Sends a message to a queue
	 * @param url of th queue
	 * @param body body of the message
	 * @return message id
	 */
	public String sendMessage(String url, String body);
	
	/**
	 * Deletes a message
	 * @param message
	 * @param queueUrl
	 */
	public void deleteMessage(Message message, String queueUrl);

	/**
	 * Creates a queue
	 * @param queueName
	 * @return created queue url
	 */
	public String createQueue(String queueName);
   
	/**
	 * Get queues with prefix
	 * @param prefix
	 * @return list of queue urls
	 */
	public List<String> listQueues(String prefix);
	
	/**
	 * Get queues
	 * @return list of queue urls
	 */
	public List<String> listQueues();
	
	/**
	 * Purge queue
	 */
	public void deleteQueue(String url);
	
	public SQSQueuePoller getPoller(String queueUrl);
	
}
