package com.example.sandbox.config;

import com.example.sandbox.avro.User;
import com.example.sandbox.dto.Customer;
import io.confluent.kafka.serializers.*;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class AppConfig {
    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("topic1")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topic2() {
        return TopicBuilder.name("topic2")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topic3() {
        return TopicBuilder.name("topic3")
                .partitions(3)
                .replicas(1)
                .build();
    }

   	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	public Map<String, Object> consumerConfigString() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, "dnk");
		return props;
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactoryString() {
		return new DefaultKafkaConsumerFactory<>(
				consumerConfigString(),
				new StringDeserializer(),
				new StringDeserializer());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryString() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryString());
		return factory;
	}

    ////////////////////////////

	public Map<String, Object> producerConfigs2() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return props;
	}

	@Bean
	public ProducerFactory<String, Customer> producerFactory2() {
		return new DefaultKafkaProducerFactory<>(producerConfigs2());
	}

	@Bean
	public KafkaTemplate<String, Customer> kafkaTemplate2() {
		return new KafkaTemplate<String, Customer>(producerFactory2());
	}


	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "dnk");
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		return props;
	}

	@Bean
	public ConsumerFactory<String, Customer> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(
				consumerConfigs(),
				new StringDeserializer(),
				new JsonDeserializer<>(Customer.class));
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Customer> kafkaListenerContainerFactoryJson() {
		ConcurrentKafkaListenerContainerFactory<String, Customer> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}


    ////////////////////
    public Map<String, Object> producerConfigAvro() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:18081");
        return props;
    }

    @Bean
    public ProducerFactory<String, User> producerFactoryAvro() {
        return new DefaultKafkaProducerFactory<>(producerConfigAvro());
    }

    @Bean
    public KafkaTemplate<String, User> kafkaTemplateAvro() {
        return new KafkaTemplate<>(producerFactoryAvro());
    }


	public Map<String, Object> consumerConfigAvro() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "dnk23");
		props.put( AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");
		props.put( KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true );
		props.put( KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());

		return props;
	}

	@Bean
	public ConsumerFactory<String, Object> consumerFactoryAvro() {
		KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
		kafkaAvroDeserializer.configure(consumerConfigAvro(), false);
		return new DefaultKafkaConsumerFactory(consumerConfigAvro(),
				new StringDeserializer(),
				kafkaAvroDeserializer);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactoryAvro() {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryAvro());
		return factory;
	}

}
