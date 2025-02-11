package guc.bttsBtngan.post.amqp;

import java.util.HashMap;
import java.util.Map;

import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.ResourceExistsException;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.QueueProperties;
import com.azure.spring.cloud.autoconfigure.implementation.servicebus.properties.AzureServiceBusProperties;
import com.azure.spring.messaging.servicebus.core.ServiceBusTemplate;
import com.azure.spring.messaging.servicebus.implementation.core.annotation.ServiceBusListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;

import guc.bttsBtngan.post.commands.Command;
import org.springframework.messaging.support.MessageBuilder;


@Configuration
public class RabbitMQConfig {

    @Autowired
    private Map<String, Command> commands;
    @Autowired
    private ServiceBusTemplate serviceBusTemplate;
//    	@Autowired
//	private ExecutorService threadPool;
    private static final String request_queue = "post_req";
	public static final String reply_queue = "gateway";
	public static final String temporary_reply_queue = "temporary_reply_queue";

    @Bean
    ServiceBusAdministrationClient adminClient(TokenCredential tokenCredential, AzureServiceBusProperties properties) {
        if (properties.getNamespace() == null || properties.getDomainName() == null) {
            throw new IllegalArgumentException("Namespace and domainName must not be null");
        }
        return new ServiceBusAdministrationClientBuilder()
            .credential(properties.getNamespace() + "." + properties.getDomainName(), tokenCredential)
            .buildClient();
    }

    @Bean(name = {request_queue})
    QueueProperties requestQueueProperties(ServiceBusAdministrationClient adminClient) {
        try {
            return adminClient.createQueue(request_queue);
        } catch (ResourceExistsException e) {
            return adminClient.getQueue(request_queue);
        }
    }

    @Bean(name = {temporary_reply_queue})
    QueueProperties temporyQueueProperties(ServiceBusAdministrationClient adminClient) {
        try {
            return adminClient.createQueue(temporary_reply_queue);
        } catch (ResourceExistsException e) {
            return adminClient.getQueue(temporary_reply_queue);
        }
    }

//	@Bean(name = {reply_queue})
//	public Queue reply_queue() {
//		return new Queue(reply_queue);
//	}

//	@Bean
//	public ExecutorService executor() {
//		return new ThreadPoolExecutor(10, 20, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));
//	}

//    @RabbitListener(queues = request_queue)
//    public void listen(HashMap<String, Object> payload, @Headers Map<String, Object> headers) {
//    	threadPool.submit(() -> {
//        	HashMap<String, Object> map = new HashMap<>();
//        	try {
//        		System.out.println("started processing task: " + payload.get("content"));
//        		payload.put("user_id", headers.get("user_id"));
//				payload.put("timestamp", headers.get("timestamp"));
//    			Object res = commands.get((String)headers.get("command")).execute(payload);
//    			map.put("data", res);
//    		} catch (Exception e) {
//    			map.put("error", e.getMessage());
//    		} finally {
//    			amqpTemplate.convertAndSend((String) headers.get("amqp_replyTo"), map, m -> {
//    	        	m.getMessageProperties().setCorrelationId((String) headers.get("amqp_correlationId"));
//    	        	m.getMessageProperties().setReplyTo((String) headers.get("amqp_replyTo"));
//    	        	return m;
//    			});
//        		System.out.println("finished processing task: " + payload.get("content"));
//    		}
//    	});
//    }

    @ServiceBusListener(destination = request_queue)
    public void listen(HashMap<String, Object> payload, @Headers Map<String, Object> headers) {
        HashMap<String, Object> map = new HashMap<>();
        try {
            payload.put("user_id", headers.get("user_id"));
            payload.put("timestamp", headers.get("timestamp").toString());
            Object res = commands.get((String)headers.get("command")).execute(payload);
            map.put("data", res);
        } catch (Exception e) {
            map.put("error", e.getMessage());
        } finally {
            serviceBusTemplate.send((String) headers.get(MessageHeaders.REPLY_CHANNEL),
                MessageBuilder
                    .withPayload(map)
                    .setHeader(MessageHeaders.REPLY_CHANNEL, headers.get(MessageHeaders.REPLY_CHANNEL))
                    .build());
        }
    }

    // dummy method for testing
    @Bean
    public ApplicationRunner runner(ServiceBusTemplate template, ServiceBusSessionReceiverClient receiverClient) {
        return args -> {
        	for(char c='a' ; c<'z'; c++) {
            	Map<String, Object> map = new HashMap<>();
            	map.put("userId", "user_1");
            	map.put("content", "hi"+c);
                serviceBusTemplate.send(request_queue,
                        MessageBuilder
                                .withPayload(map)
                                .setHeader("command", "searchPostCommand")
                                .setHeader(MessageHeaders.REPLY_CHANNEL, RabbitMQConfig.reply_queue)
                                .build());

                // Accept the session (waits for the session to exist)
                ServiceBusReceiverClient command_receiver = receiverClient.acceptSession("searchPostCommand");

                // Receive the reply (only one message in this session)
                ServiceBusReceivedMessage command_reply = command_receiver.receiveMessages(1)
                        .stream()
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("No reply received"));
                command_receiver.complete(command_reply);
                command_receiver.close();
                System.out.println(command_reply+" "+c);
        	}
        };
    }




}
