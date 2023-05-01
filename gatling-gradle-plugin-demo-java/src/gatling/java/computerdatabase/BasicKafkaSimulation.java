package computerdatabase;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import ru.tinkoff.gatling.kafka.javaapi.protocol.KafkaProtocolBuilder;
import ru.tinkoff.gatling.kafka.javaapi.protocol.KafkaProtocolBuilderNew;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.*;
import static ru.tinkoff.gatling.kafka.javaapi.KafkaDsl.kafka;


/**
 * Tested on 172.16.92.197
 * This sample is based on our official tutorials:
 * <ul>
 *   <li><a href="https://gatling.io/docs/gatling/tutorials/quickstart">Gatling quickstart tutorial</a>
 *   <li><a href="https://gatling.io/docs/gatling/tutorials/advanced">Gatling advanced tutorial</a>
 * </ul>
 */
public class BasicKafkaSimulation extends Simulation {
    private final  KafkaProtocolBuilderNew kafkaProtocolC = kafka().requestReply()
                                                                   .producerSettings(
                                                                           Map.of(
                                                                                   ProducerConfig.ACKS_CONFIG, "1",
                                                                                   ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
                                                                                 )
                                                                                    )
                                                                   .consumeSettings(
                                                                           Map.of("bootstrap.servers", "localhost:9092")
                                                                                   ).timeout(Duration.ofSeconds(5));

    private final AtomicInteger c = new AtomicInteger(0);

    private final Iterator<Map<String, Object>> feeder =
            Stream.generate((Supplier<Map<String, Object>>) () -> Collections.singletonMap("kekey", c.incrementAndGet())
                           ).iterator();

    private final Headers headers = new RecordHeaders().add("test-header", "test_value".getBytes());

    private final ScenarioBuilder scn = scenario("Basic")
            .feed(feeder)
            .exec(
                    kafka("ReqRep").requestReply()
                                   .requestTopic("test.t")
                                   .replyTopic("test.t")
                                   .send("#{kekey}", """
                            { "m": "dkf" }
                            """, headers, String.class, String.class)
                                   .check(jsonPath("$.m").is("dkf"))
                 );

    {
        setUp(scn.injectOpen(atOnceUsers(5))).protocols(kafkaProtocolC).maxDuration(Duration.ofSeconds(120));
    }
}
