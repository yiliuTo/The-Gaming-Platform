package guc.bttsBtngan.authentication.amqp;

import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.ResourceExistsException;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.QueueProperties;
import com.azure.spring.cloud.autoconfigure.implementation.servicebus.properties.AzureServiceBusProperties;
import com.azure.spring.messaging.servicebus.core.ServiceBusTemplate;
import com.azure.spring.messaging.servicebus.support.ServiceBusMessageHeaders;
import guc.bttsBtngan.authentication.commands.Command;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;

import com.azure.spring.messaging.servicebus.implementation.core.annotation.ServiceBusListener;
import org.springframework.messaging.support.MessageBuilder;


@Configuration
public class RabbitMQConfig {

    @Autowired
    private Map<String, Command> commands;
    @Autowired
    private ServiceBusTemplate serviceBusTemplate;
    private static final String request_queue = "authentication_req";
    @Autowired
	private ExecutorService threadPool;

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

	@Bean
	public ExecutorService executor() {

		return new ThreadPoolExecutor(10, 20, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));
	}

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
    public void listen_sb(HashMap<String, Object> payload, @Headers Map<String, Object> headers) {
        HashMap<String, Object> map = new HashMap<>();
        try {
            payload.put("user_id", headers.get("user_id"));
            Object res = commands.get((String)headers.get("command")).execute(payload);
            map.put("data", res);
            System.out.println("data is "+res);
        } catch (Exception e) {
            map.put("error", e.getMessage());
            System.out.println("message is "+e.getMessage());
        } finally {
            serviceBusTemplate.send((String) headers.get(MessageHeaders.REPLY_CHANNEL),
                MessageBuilder
                    .withPayload(map)
                    .setHeader(MessageHeaders.REPLY_CHANNEL, headers.get(MessageHeaders.REPLY_CHANNEL))
                    .build());
        }
    }

    // dummy method for testing
//    @Bean
//    public ApplicationRunner runner(AmqpTemplate template) {
//        return args -> {
//        	for(int i = 0 ; i < 20; i++) {
//            	Map<String, Object> map = new HashMap<>();
//            	map.put("token", "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOlJNYXplbmVsZ2FklZWQiLCJleHAiOjE2NTIxMjg5OTAsImlhdCI6MTY1MjExMDk5MH0.A_xSHip-mSMOkCduh06CR3nLu2InPLbuKmd6LVP4A9rV-depbOPffvOlrkQK5HwAc9w0IN5dwha9rvpE0xibZQ");
//            	template.convertAndSend(request_queue, map, m -> {
//                	m.getMessageProperties().setHeader("command", "verifyCommand");
//                	return m;
//                });
//        	}
//        };
//    }

//    @Bean
//    public ApplicationRunner runner2(AmqpTemplate template) {
//        return args -> {
//                Map<String, Object> map = new HashMap<>();
//                map.put("username", "Mazenelgamed");
//                map.put("password","12345");
//                template.convertAndSend(request_queue, map, m -> {
//                    m.getMessageProperties().setHeader("command", "loginCommand");
//                    return m;
//                });
//        };
//    }
//@Bean
//public ApplicationRunner runner2(AmqpTemplate template) {
//    return args -> {
//        Map<String, Object> map = new HashMap<>();
//        map.put("token","eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJNYXplbmVsZ2FtZWQiLCJleHAiOjE2NTIxMzM1MTksImlhdCI6MTY1MjExNTUxOX0.nHcHCP1P_NW-dYnmzDIo0tTlDlUdVa1-x3PRy2lcbZStjDlVwDw1nYKjuu57gbx0X0-BNSEKjDX0j-8lwf26QA");
//        template.convertAndSend(request_queue, map, m -> {
//            m.getMessageProperties().setHeader("command", "logoutCommand");
//            return m;
//        });
//    };
//}

}