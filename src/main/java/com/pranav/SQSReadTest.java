package com.pranav;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

public class SQSReadTest implements RequestHandler<SQSEvent, Void> {

	@Override
	public Void handleRequest(SQSEvent input, Context context) {
		List<SQSMessage> messages = input.getRecords();
		messages.forEach(message -> {
			System.out.println(String.format("Received message from %s with id %s and body as %s",
					message.getEventSource(), message.getMessageId(), message.getBody()));
		});
		return null;
	}
}
