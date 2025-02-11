package guc.bttsBtngan.notification.amqp;
import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.ResourceExistsException;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.QueueProperties;
import com.azure.spring.cloud.autoconfigure.implementation.servicebus.properties.AzureServiceBusProperties;
import com.azure.spring.messaging.servicebus.core.ServiceBusTemplate;
import com.azure.spring.messaging.servicebus.implementation.core.annotation.ServiceBusListener;
import guc.bttsBtngan.notification.commands.Command;

import java.util.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.support.MessageBuilder;


@Configuration
public class RabbitMQConfig {

    @Autowired
    private Map<String, Command> commands;
    @Autowired
    private ServiceBusTemplate serviceBusTemplate;
    private static final String request_queue = "notification_req";
   // private static final String reply_queue = "notification_gateway";

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

//    @Bean(name = {reply_queue})
//    public Queue reply_queue() {
//        return new Queue(reply_queue);
//    }

    @ServiceBusListener(destination = request_queue)
    public void listen(HashMap<String, Object> payload, @Headers Map<String, Object> headers) {
        HashMap<String, Object> map = new HashMap<>();
        try {
            System.out.println("started processing task: " + payload.get("type"));
            payload.put("user_id", headers.get("user_id"));
            payload.put("timestamp", headers.get("timestamp").toString());
            Object res = commands.get((String)headers.get("command")).execute(payload);
            map.put("data", res);
            System.out.println(res);
        } catch (Exception e) {
            map.put("error", e.getMessage());
            System.out.println(e.getMessage());
        } finally {
            serviceBusTemplate.send((String) headers.get(MessageHeaders.REPLY_CHANNEL),
                    MessageBuilder
                            .withPayload(map)
                            .setHeader(MessageHeaders.REPLY_CHANNEL, headers.get(MessageHeaders.REPLY_CHANNEL))
                            .build());
            System.out.println("finished processing task: " + payload.get("type"));
        }
    }

    // dummy method for testing create
//    @Bean
//    public ApplicationRunner runner(AmqpTemplate template) {
//        return args -> {
//            for(int i = 0 ; i < 20; i++) {
//                Map<String, Object> map = new HashMap<>();
//                map.put("type", "comment");
//                ArrayList<String>list=new ArrayList<String>();
//                list.add("id10"+i);
//                list.add("id11"+i+1);
//                list.add("id12"+i+2);
//                list.add("id13"+i+3);
//                map.put("userIDs", list);
//                template.convertAndSend(request_queue, map, m -> {
//                    m.getMessageProperties().setHeader("command", "createNotificationCommand");
//                    return m;
//                });
//            }
//        };
//    }

//    // dummy method for testing update
//    @Bean
//    public ApplicationRunner runner(AmqpTemplate template) {
//        return args -> {
//            for(int i = 0 ; i < 20; i++) {
//                Map<String, Object> map = new HashMap<>();
//
//                map.put("notificationID", "0VQssXEceOufv7hAUHqT");
//                map.put("type", "comment"+i);
//                ArrayList<String>list=new ArrayList<String>();
//                list.add("id10"+i);
//                list.add("id11"+i+1);
//                list.add("id12"+i+2);
//                list.add("id13"+i+3);
//                map.put("userIDs", list);
//                template.convertAndSend(request_queue, map, m -> {
//                    m.getMessageProperties().setHeader("command", "updateNotificationCommand");
//                    return m;
//                });
//            }
//        };
//    }

//    // dummy method for testing delete
//    @Bean
//    public ApplicationRunner runner(AmqpTemplate template) {
//        return args -> {
//            for(int i = 0 ; i < 20; i++) {
//                Map<String, Object> map = new HashMap<>();
//
//                map.put("notificationID", "0VQssXEceOufv7hAUHqT");
//                map.put("userID", "id12142");
//
//                template.convertAndSend(request_queue, map, m -> {
//                    m.getMessageProperties().setHeader("command", "deleteNotificationCommand");
//                    return m;
//                });
//            }
//        };
//    }

    // dummy method for testing get
//    @Bean
//    public ApplicationRunner runner(AmqpTemplate template) {
//        return args -> {
//            for(int i = 0 ; i < 5; i++) {
//                Map<String, Object> map = new HashMap<>();
//
//            //    map.put("notificationID", "0VQssXEceOufv7hAUHqT");
//                map.put("userID", "id1015");
//
//                template.convertAndSend(request_queue, map, m -> {
//                    m.getMessageProperties().setHeader("command", "getNotificationCommand");
//                    return m;
//                });
//            }
//        };
//    }

}
