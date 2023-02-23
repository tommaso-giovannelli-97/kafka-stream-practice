import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColor {
    final String INPUT_TOPIC_NAME = "favourite-color-input";
    final String INTERMEDIATE_TOPIC_NAME = "user-keys-and-colors";
    final String OUTPUT_TOPIC_NAME = "favourite-color-output";

    public Topology createTopology(){
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        StreamsBuilder builder = new StreamsBuilder();

        //Publishing to intermediate topic
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC_NAME);
        KStream<String, String> usersAndColors = textLines
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));
        usersAndColors.to(INTERMEDIATE_TOPIC_NAME, Produced.with(stringSerde, stringSerde));

        //Publishing to final topic
        KTable<String, String> usersAndColorsTable = builder.table(INTERMEDIATE_TOPIC_NAME);
        KTable<String, Long> favouriteColors = usersAndColorsTable
                // 5 - we group by color within the KTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColors")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        // 6 - we output the results to a Kafka Topic - don't forget the serializers
        favouriteColors.toStream().to(OUTPUT_TOPIC_NAME, Produced.with(Serdes.String(),Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-java");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KafkaStreams streams = new KafkaStreams(new FavouriteColor().createTopology(), config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(System.out::println);

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
