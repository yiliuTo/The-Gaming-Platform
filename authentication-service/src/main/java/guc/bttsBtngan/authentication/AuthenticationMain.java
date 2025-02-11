package guc.bttsBtngan.authentication;

import com.azure.spring.messaging.implementation.annotation.EnableAzureMessaging;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
@EnableAzureMessaging
public class AuthenticationMain {

	public static void main(String[] args) {
		SpringApplication.run(AuthenticationMain.class, args);
	}

}
