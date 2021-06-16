package tech.central.ai.esb.kstreamsample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import tech.central.ai.esb.kstreamsample.model.OfferRecord;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.function.Function;

@SpringBootApplication
public class KstreamSampleApplication {

    private static final String STORE_NAME = "latest-offer-store";

    private static final Logger logger = LoggerFactory.getLogger(KstreamSampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KstreamSampleApplication.class, args);
    }

    @Bean
    public Function<KStream<String, OfferRecord.Data>, KStream<String, String>> process() {

        return input -> input
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(300)))
                .reduce((previous, next) -> {
                    String message = MessageFormat.format("previous: {0}, price={2}, next {1}, price={3}",
                            previous.getLastUpdated(),
                            next.getLastUpdated(),
                            previous.getPrice(),
                            next.getPrice()
                    );
                    logger.info(message);
                    if (next.getLastUpdated() == null || previous.getLastUpdated() == null) {
                        return next;
                    }
                    return next.getLastUpdated().after(previous.getLastUpdated()) ? next : previous;
                }, Materialized.as(STORE_NAME))
                .toStream()
                .map(this::serializeOutput);
    }


    private KeyValue<String, String> serializeOutput(Windowed<String> key, OfferRecord.Data value) {
        String message = MessageFormat.format(
                "Latest value of {0} = {1}, offer:{2} at {3}",
                key.key(),
                value.getPrice(),
                value.getOfferId(),
                value.getLastUpdated()
        );
        logger.info(message);

        return new KeyValue<>(key.key(), message);
    }
}
