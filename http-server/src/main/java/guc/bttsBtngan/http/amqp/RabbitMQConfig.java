package guc.bttsBtngan.http.amqp;

import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.ResourceExistsException;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.QueueProperties;
import com.azure.spring.cloud.autoconfigure.implementation.servicebus.properties.AzureServiceBusProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
	
	public static final String reply_queue = "http_queue";
	
	
	@Bean(name = {reply_queue})
	public Queue reply_queue() {
		return new Queue(reply_queue);
	}

	@Bean
	ServiceBusAdministrationClient adminClient(TokenCredential tokenCredential, AzureServiceBusProperties properties) {
		if (properties.getNamespace() == null || properties.getDomainName() == null) {
			throw new IllegalArgumentException("Namespace and domainName must not be null");
		}
		return new ServiceBusAdministrationClientBuilder()
				.credential(properties.getNamespace() + "." + properties.getDomainName(), tokenCredential)
				.buildClient();
	}

	@Bean(name = {reply_queue})
	QueueProperties replyQueueProperties(ServiceBusAdministrationClient adminClient) {
		try {
			return adminClient.createQueue(reply_queue);
		} catch (ResourceExistsException e) {
			return adminClient.getQueue(reply_queue);
		}
	}
	
}
