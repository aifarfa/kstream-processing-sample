package tech.central.ai.esb.kstreamsample;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.text.MessageFormat;
import java.util.function.Consumer;

@SpringBootApplication
public class KstreamSampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(KstreamSampleApplication.class, args);
	}

	@Bean
	public Consumer<KStream<String, String>> process() {
		return input -> input.foreach((key, value) -> {
			System.out.println(MessageFormat.format("received {0}: {1}", key, value));
		});
	}

}
