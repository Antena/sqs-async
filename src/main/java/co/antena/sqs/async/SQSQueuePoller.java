package co.antena.sqs.async;

public interface SQSQueuePoller {

	public void start();

	public void subscribe(SQSMessageHandler sqsMessageHandler);

}
