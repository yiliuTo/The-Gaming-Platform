package guc.bttsBtngan.http.controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.spring.messaging.servicebus.core.ServiceBusProcessorFactory;
import com.azure.spring.messaging.servicebus.core.ServiceBusTemplate;
import com.azure.spring.messaging.servicebus.core.listener.ServiceBusMessageListenerContainer;
import com.azure.spring.messaging.servicebus.core.properties.ServiceBusContainerProperties;
import com.azure.spring.messaging.servicebus.support.ServiceBusMessageHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import guc.bttsBtngan.http.amqp.RabbitMQConfig;

@RestController
public class Controller {
	
	private ServiceBusTemplate serviceBusTemplate;
	private Map<String, String> serviceToCommand;
	private ServiceBusSessionReceiverClient  receiverClient;
	
	@Autowired
	public Controller(ServiceBusTemplate serviceBusTemplate, ServiceBusSessionReceiverClient receiverClient) {
		this.serviceBusTemplate = serviceBusTemplate;
		Map<String, String> serviceToCommand = new HashMap<>();
		serviceToCommand.put("chat", "messaging_req");
		serviceToCommand.put("authentication", "authentication_req");
		serviceToCommand.put("notification", "notification_req");
		serviceToCommand.put("user", "user_req");
		serviceToCommand.put("post", "post_req");
		this.serviceToCommand = serviceToCommand;
		this.receiverClient = receiverClient;
	}


	@SuppressWarnings("unchecked")
	@PostMapping("/")
	public Map<String, Object> handler(@RequestBody Map<String, Object> body,
			@RequestHeader Map<String, String> headers, HttpServletResponse servletResponse) {

		String[] route = headers.get("routing-key").split("\\.");
		String service = route[0], command = route[1];
		Map<String, Object> res = null;
		if(!("loginCommand".equals(command) || "registerUserCommand".equals(command))) {
			Map<String, Object> auth_body = new HashMap<>();
			auth_body.put("token", headers.get("token-x"));
			String sessionId = UUID.randomUUID().toString();
			serviceBusTemplate.send(
				serviceToCommand.get("authentication"),
				MessageBuilder.withPayload(auth_body)
					.setHeader("command", "verifyCommand")
					.setHeader(MessageHeaders.REPLY_CHANNEL, RabbitMQConfig.reply_queue)
					.setHeader(ServiceBusMessageHeaders.SESSION_ID, sessionId)
					.build());
			// Accept the session (waits for the session to exist)
			ServiceBusReceiverClient receiver = receiverClient.acceptSession(sessionId);

			try {
				// Receive the reply (only one message in this session)
				ServiceBusReceivedMessage reply = receiver.receiveMessages(1)
						.stream()
						.findFirst()
						.orElseThrow(() -> new RuntimeException("No reply received"));
				if reply
				receiver.complete(reply);
			} finally {
				receiver.close();
				sessionReceiver.close();
				sender.close();
			}

			if(auth_res.get("error") != null) {
				servletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
				return auth_res;
			}
			res = (Map<String, Object>) amqpTemplate.convertSendAndReceive(
					serviceToCommand.get(service), body, m -> {
	        	m.getMessageProperties().setHeader("command", command);
	        	m.getMessageProperties().setHeader("user_id", auth_res.get("data").toString());
	    		m.getMessageProperties().setReplyTo(RabbitMQConfig.reply_queue);

	        	return m;
	        });
		}
		else {
			res = (Map<String, Object>) amqpTemplate.convertSendAndReceive(
					serviceToCommand.get(service), body, m -> {
	        	m.getMessageProperties().setHeader("command", command);
	    		m.getMessageProperties().setReplyTo(RabbitMQConfig.reply_queue);

	        	return m;
	        });
		}

		if(res.get("error") != null) {
			servletResponse.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
		}
		return res;	
	}

}
