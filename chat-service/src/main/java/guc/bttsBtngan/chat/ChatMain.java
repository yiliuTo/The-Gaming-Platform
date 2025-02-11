package guc.bttsBtngan.chat;

import com.azure.spring.messaging.implementation.annotation.EnableAzureMessaging;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
@EnableAzureMessaging
public class ChatMain {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(ChatMain.class, args);
	}

}
