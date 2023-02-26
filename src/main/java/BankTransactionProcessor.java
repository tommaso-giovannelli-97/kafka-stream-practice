
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BankTransactionProcessor {
    final String INPUT_TOPIC = "bank-transactions";
    final String OUTPUT_TOPIC = "bank-balance";

    public Topology createTopology(){
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> bankTransactions = builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), jsonSerde));

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey()
                .aggregate(
                        BankTransactionProcessor::initializeBalance,
                        (key, value, aggValue) -> generateNewBalance(value, aggValue)
                ,Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde));

        bankBalance.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), jsonSerde));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        KafkaStreams streams = new KafkaStreams(new BankTransactionProcessor().createTopology(), config);
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(System.out::println);

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static JsonNode initializeBalance() {
        ObjectNode balance = JsonNodeFactory.instance.objectNode();
        balance.put("count", 0);
        balance.put("balance", 0);
        balance.put("time", Instant.ofEpochMilli(0L).toString());
        return balance;
    }

    private static JsonNode generateNewBalance(JsonNode transaction, JsonNode oldBalance) {
        // create a new balance json object
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", oldBalance.get("count").asInt() + 1);
        newBalance.put("balance", oldBalance.get("balance").asInt() + transaction.get("amount").asInt());

        long balanceEpoch = Instant.parse(oldBalance.get("time").asText()).toEpochMilli();
        long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
