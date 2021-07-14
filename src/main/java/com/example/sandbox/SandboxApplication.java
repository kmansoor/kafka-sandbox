package com.example.sandbox;



import com.example.sandbox.avro.User;
import com.example.sandbox.dto.Customer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RestController
@SpringBootApplication
public class SandboxApplication {

	public static void main(String[] args) {
		SpringApplication.run(SandboxApplication.class, args);
	}

	@GetMapping("/")
	public String hello() {
		return "Hello";
	}



	@KafkaListener(id = "myId", topics = "topic1", containerFactory = "kafkaListenerContainerFactoryString")
	public void listen(@Payload String in,
					   @Header("X-Custom-Header") String customHeader) {
		System.out.println("<<<String>>> " + in + " " + customHeader);
	}

	@KafkaListener(id = "myId2", topics = "topic2", containerFactory = "kafkaListenerContainerFactoryJson")
	public void listen2(@Payload Customer c,
			@Header("X-Custom-Header") String customHeader) {
		System.out.println("<<<Json>>> " + c.getName() + " " + c.getNumber() + " " + customHeader);
	}

	@KafkaListener(id = "myId3", topics = "topic3", containerFactory="kafkaListenerContainerFactoryAvro")
	public void listen3(@Payload User rec
						,@Header("X-APP-EVENT") String customHeader
	) {
		System.out.println("<<<Avro>>> " + rec.getName() + " " + rec.getNumber() + " " + customHeader);
	}

	@Component
	public static class MyRunner implements ApplicationRunner {

		private final KafkaTemplate<String, String> kafkaTemplate;
		private final KafkaTemplate<String, Customer> kafkaTemplate2;
		private final KafkaTemplate<String, User> kafkaTemplate3;
		private int count = 1;
		public MyRunner(
				KafkaTemplate<String, String> kafkaTemplate,
				KafkaTemplate<String, Customer> kafkaTemplate2,
				KafkaTemplate<String, User> kafkaTemplate3) {
			this.kafkaTemplate = kafkaTemplate;
			this.kafkaTemplate2 = kafkaTemplate2;
			this.kafkaTemplate3 = kafkaTemplate3;
		}

		@Override
		public void run(ApplicationArguments args) {
			Runnable runnable = () -> {
//				System.out.println("Hello");

				Message<String> message = MessageBuilder
						.withPayload("Test String Message " + count++)
						.setHeader(KafkaHeaders.TOPIC, "topic1")
						.setHeader(KafkaHeaders.MESSAGE_KEY, "999")
						.setHeader(KafkaHeaders.PARTITION_ID, 0)
						.setHeader("X-Custom-Header", "Sending Custom Header with Spring Kafka")
						.build();

//				kafkaTemplate.send("topic1", ">>> Hello " + count++);
				kafkaTemplate.send(message);

				///////////////////////////////////////////
				Customer c = new Customer();
				c.setName("Jane Dank");
				c.setColor("Blue");
				c.setNumber(String.valueOf(count));

				Message<Customer> message2 = MessageBuilder
						.withPayload(c)
						.setHeader(KafkaHeaders.TOPIC, "topic2")
						.setHeader(KafkaHeaders.MESSAGE_KEY, "999")
						.setHeader(KafkaHeaders.PARTITION_ID, 0)
						.setHeader("X-Custom-Header", "Sending Custom Header - Dank")
						.build();

				kafkaTemplate2.send(message2);

				try {
					User user = User.newBuilder()
							.setName("Henry Green Engine")
							.setNumber(count)
							.build();

					Message<User> message3 = MessageBuilder
							.withPayload(user)
							.setHeader(KafkaHeaders.TOPIC, "topic3")
							.setHeader(KafkaHeaders.MESSAGE_KEY, "some-key")
							.setHeader(KafkaHeaders.PARTITION_ID, 0)
							.setHeader("X-APP-EVENT", "ApplicationCreatedEvent")
							.build();

					kafkaTemplate3.send(message3);
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
			};

			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable,1, 10, TimeUnit.SECONDS);
//			Executors.callable(runnable);
		}
	}

}
