package co.antena.sqs.async;

import com.amazonaws.services.sqs.model.Message;

public interface SQSMessageHandler {

	public boolean handleMessage(Message message);
	
}
