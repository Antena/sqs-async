package co.antena.sqs.async;

import java.util.List;

import com.amazonaws.services.sqs.model.Message;

/**
 * Abstracts an Amazon SQS SDK implementation
 *
 * @author gonzalo
 *
 */
public interface SQSProvider {

	/**
	 * Creates a queue
	 *
	 * @param queueName
	 * @return created queue url
	 */
	public String createQueue(String queueName);

	/**
	 * Deletes a message
	 *
	 * @param message
	 * @param queueUrl
	 */
	public void deleteMessage(Message message, String queueUrl);

	void deleteMessageBatch(List<Message> messages, String queueUrl);

	/**
	 * Purge queue
	 */
	public void deleteQueue(String url);

	public SQSQueuePoller getPoller(String queueUrl);

	/**
	 * Get queues
	 *
	 * @return list of queue urls
	 */
	public List<String> listQueues();

	/**
	 * Get queues with prefix
	 *
	 * @param prefix
	 * @return list of queue urls
	 */
	public List<String> listQueues(String prefix);

	/**
	 * Receives a message with long polling
	 *
	 * @param receiveMessageRequest
	 * @return
	 */
	public List<Message> receiveLPMessage(String receiveMessageRequest);

	List<Message> receiveLPMessage(String url, int qMessages);

	/**
	 * Receives a message
	 *
	 * @param receiveMessageRequest
	 * @return
	 */
	public List<Message> receiveMessage(String receiveMessageRequest);

	List<Message> receiveMessage(String url, int qMessages);

	/**
	 * Sends a message to a queue
	 *
	 * @param url
	 *            of th queue
	 * @param body
	 *            body of the message
	 * @return message id
	 */
	public String sendMessage(String url, String body);

}
