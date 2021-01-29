package com.learning.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerBasicConfigurations {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerBasicConfigurations.class);

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("first_topic", "Hare Krishna");
		
		kafkaProducer.send(new ProducerRecord<String, String>("GodName", "Hare Krishna"));
		kafkaProducer.send(record1);
		try {
			Thread.sleep(10L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//kafkaProducer.flush();
		//kafkaProducer.close();

			}

}
