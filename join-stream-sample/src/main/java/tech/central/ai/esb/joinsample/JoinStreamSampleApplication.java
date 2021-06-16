package tech.central.ai.esb.joinsample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.function.BiFunction;

@SpringBootApplication
public class JoinStreamSampleApplication {

    private static final Logger logger = LoggerFactory.getLogger(JoinStreamSampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(JoinStreamSampleApplication.class, args);
    }

    @Bean
    public BiFunction<KStream<String, String>, KStream<String, String>, KStream<String, String>> joinProcess() {
        return (sourceUpdated, productCreated) -> {
//            var productTable = productCreated.toTable(
//                    Named.as("product-created-table"),
//                    Materialized.as("product-created-store"));

            var joinWindows = JoinWindows.of(Duration.ofMinutes(3));
            ValueJoiner<String, String, String> joiner = (String source, String product) -> source;

            return sourceUpdated
                    .join(productCreated, joiner, joinWindows)
                    .map((sku, value) -> {
                        logger.debug("received source update for {}, {}", sku, value);
                        var result = MessageFormat.format("received source update for {0}: {1}", sku, value);
                        return new KeyValue<>(sku, result);
                    });
        };
    }
}
