package guc.bttsBtngan.post;

import com.azure.spring.messaging.implementation.annotation.EnableAzureMessaging;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAzureMessaging
public class PostMain {
	public static void main(String[] args) {
		SpringApplication.run(PostMain.class, args);
	}
}
