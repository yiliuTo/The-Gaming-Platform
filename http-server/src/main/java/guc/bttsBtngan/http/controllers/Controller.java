package guc.bttsBtngan.http.controllers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.spring.messaging.servicebus.core.ServiceBusTemplate;
import com.azure.spring.messaging.servicebus.support.ServiceBusMessageHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

	@Value("${spring.rabbitmq.channel-rpc-timeout}")
	private long rpcTimeout;
	
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
			serviceBusTemplate.send(
				serviceToCommand.get("authentication"),
				MessageBuilder.withPayload(auth_body)
					.setHeader("command", "verifyCommand")
					.setHeader(MessageHeaders.REPLY_CHANNEL, RabbitMQConfig.reply_queue)
					.setHeader(ServiceBusMessageHeaders.SESSION_ID, "verifyCommand")
					.build());
			// Accept the session (waits for the session to exist)
			ServiceBusReceiverClient auth_receiver = receiverClient.acceptSession("verifyCommand");

			// Receive the reply (only one message in this session)
			ServiceBusReceivedMessage auth_reply = auth_receiver.receiveMessages(1, Duration.ofMillis(rpcTimeout))
					.stream()
					.findFirst()
					.orElseThrow(() -> new RuntimeException("No reply received"));
			auth_receiver.complete(auth_reply);
			HashMap<String, Object> auth_res = auth_reply.getBody().toObject(HashMap.class);
			if (auth_res.containsKey("error")) {
				servletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
				return auth_res;
			}
			auth_receiver.close();

			serviceBusTemplate.send(
				serviceToCommand.get(service), MessageBuilder.withPayload(body)
					.setHeader("command", command)
					.setHeader("user_id", auth_res.get("data").toString())
					.setHeader(MessageHeaders.REPLY_CHANNEL, RabbitMQConfig.reply_queue)
					.setHeader(ServiceBusMessageHeaders.SESSION_ID, command)
					.build());

			// Accept the session (waits for the session to exist)
			ServiceBusReceiverClient command_receiver = receiverClient.acceptSession(command);

			// Receive the reply (only one message in this session)
			ServiceBusReceivedMessage command_reply = command_receiver.receiveMessages(1, Duration.ofMillis(rpcTimeout))
					.stream()
					.findFirst()
					.orElseThrow(() -> new RuntimeException("No reply received"));
			command_receiver.complete(command_reply);
			command_receiver.close();
			res = command_reply.getBody().toObject(HashMap.class);
		}
		else {
			serviceBusTemplate.send(
					serviceToCommand.get(service),
					MessageBuilder.withPayload(body)
							.setHeader("command", command)
							.setHeader(MessageHeaders.REPLY_CHANNEL, RabbitMQConfig.reply_queue)
							.setHeader(ServiceBusMessageHeaders.SESSION_ID, command)
							.build());

			// Accept the session (waits for the session to exist)
			ServiceBusReceiverClient command_receiver = receiverClient.acceptSession(command);

			// Receive the reply (only one message in this session)
			ServiceBusReceivedMessage command_reply = command_receiver.receiveMessages(1, Duration.ofMillis(rpcTimeout))
					.stream()
					.findFirst()
					.orElseThrow(() -> new RuntimeException("No reply received"));
			command_receiver.complete(command_reply);
			command_receiver.close();
			res = command_reply.getBody().toObject(HashMap.class);
		}

		if(res.get("error") != null) {
			servletResponse.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
		}
		return res;	
	}

}
