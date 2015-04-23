package co.antena.sqs.async;

import com.amazonaws.services.sqs.model.Message;

public interface SQSMessageHandler {

	public void handleMessage(Message message) throws SQSMessageHandlerException;
	
}
