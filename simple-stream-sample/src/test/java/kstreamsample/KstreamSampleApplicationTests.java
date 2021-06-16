package kstreamsample;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import tech.central.ai.esb.kstreamsample.KstreamSampleApplication;
import tech.central.ai.esb.kstreamsample.model.OfferRecord;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.startsWith;

@SpringBootTest(
        classes = KstreamSampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class KstreamSampleApplicationTests {

    @Autowired
    private Function<KStream<String, OfferRecord.Data>, KStream<String, String>> processor;

    private TopologyTestDriver testDriver;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Serde<String> stringSerde = Serdes.String();

    private final JsonSerde<OfferRecord.Data> dataSerde = new JsonSerde<>(OfferRecord.Data.class);

    @BeforeEach
    void setup() {
        // tricky trick setTrustedPackages for Deserializer
        Map<String, String> serdeConfig = Map.of(JsonDeserializer.TRUSTED_PACKAGES, "*");
        dataSerde.configure(serdeConfig, true);

        // using DSL to build test stream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OfferRecord.Data> input = builder.stream("sample-input", Consumed.with(stringSerde, dataSerde));

        // apply stream processor
        processor.apply(input).to("sample-output", Produced.with(stringSerde, stringSerde));

        // setup test driver
        Topology topology = builder.build();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testSequentialOrder() throws JsonProcessingException {
        // input
        OfferRecord.Data previous = new OfferRecord.Data() {{
            setOfferId("123");
            setPrice(new BigDecimal(2000));
            setLastUpdated(Date.from(Instant.parse("2020-11-23T00:00:00.00Z")));
        }};

        OfferRecord.Data next = new OfferRecord.Data() {{
            setOfferId("777");
            setPrice(new BigDecimal(1500));
            setLastUpdated(Date.from(Instant.parse("2020-11-23T00:01:20.00Z")));
        }};

        OfferRecord.Data other = new OfferRecord.Data() {{
            setOfferId("555");
            setPrice(new BigDecimal(2900));
            setLastUpdated(Date.from(Instant.parse("2020-11-23T00:01:30.00Z")));
        }};

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(
                "sample-input",
                stringSerde.serializer(),
                stringSerde.serializer());

        inputTopic.pipeInput("FOO", objectMapper.writeValueAsString(previous));
        inputTopic.pipeInput("BAR", objectMapper.writeValueAsString(other));
        inputTopic.pipeInput("FOO", objectMapper.writeValueAsString(next));

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(
                "sample-output",
                stringSerde.deserializer(),
                stringSerde.deserializer());

        List<String> actual = outputTopic.readValuesToList();

        assertThat(actual, containsInRelativeOrder(
                startsWith("Latest value of FOO = 2,000, offer:123 at 11/23/20, 7:00 AM"),
                startsWith("Latest value of BAR = 2,900, offer:555 at 11/23/20, 7:01 AM"),
                startsWith("Latest value of FOO = 1,500, offer:777 at 11/23/20, 7:01 AM")
        ));
    }

    @Test
    void testOutOfOrderMessages() throws JsonProcessingException {
        // input
        OfferRecord.Data previous = new OfferRecord.Data() {{
            setOfferId("123");
            setPrice(new BigDecimal(2000));
            setLastUpdated(Date.from(Instant.parse("2020-11-23T00:00:00.00Z")));
        }};

        OfferRecord.Data next = new OfferRecord.Data() {{
            setOfferId("777");
            setPrice(new BigDecimal(1500));
            setLastUpdated(Date.from(Instant.parse("2020-11-23T00:01:20.00Z")));
        }};

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(
                "sample-input",
                stringSerde.serializer(),
                stringSerde.serializer());

        inputTopic.pipeInput("FOO", objectMapper.writeValueAsString(next));
        inputTopic.pipeInput("FOO", objectMapper.writeValueAsString(previous));

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(
                "sample-output",
                stringSerde.deserializer(),
                stringSerde.deserializer());

        List<String> actual = outputTopic.readValuesToList();

        assertThat(actual, containsInRelativeOrder(
                startsWith("Latest value of FOO = 1,500, offer:777 at 11/23/20, 7:01 AM"),
                startsWith("Latest value of FOO = 1,500, offer:777 at 11/23/20, 7:01 AM")
        ));
    }
}
