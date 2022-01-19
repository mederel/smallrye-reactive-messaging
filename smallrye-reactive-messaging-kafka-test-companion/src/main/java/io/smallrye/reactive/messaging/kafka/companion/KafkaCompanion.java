package io.smallrye.reactive.messaging.kafka.companion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import io.smallrye.mutiny.Uni;

public class KafkaCompanion implements AutoCloseable {

    private final Map<Class<?>, Serde<?>> serdeMap = new HashMap<>();
    private final Map<String, Object> commonClientConfig = new HashMap<>();
    private final String bootstrapServers;
    private final Duration kafkaApiTimeout;
    private AdminClient adminClient;

    public KafkaCompanion(String bootstrapServers) {
        this(bootstrapServers, Duration.ofSeconds(10));
    }

    public KafkaCompanion(String bootstrapServers, Duration kafkaApiTimeout) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaApiTimeout = kafkaApiTimeout;
    }

    public Duration getKafkaApiTimeout() {
        return kafkaApiTimeout;
    }

    public Map<String, Object> getCommonClientConfig() {
        return commonClientConfig;
    }

    public void setCommonClientConfig(Map<String, Object> properties) {
        this.commonClientConfig.putAll(properties);
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public synchronized AdminClient getOrCreateAdminClient() {
        if (adminClient == null) {
            Map<String, Object> configMap = new HashMap<>(getCommonClientConfig());
            configMap.put(BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
            adminClient = AdminClient.create(configMap);
        }
        return adminClient;
    }

    @Override
    public synchronized void close() {
        if (adminClient != null) {
            adminClient.close(kafkaApiTimeout);
        }
    }

    public static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static String getHeader(Headers headers, String key) {
        return new String(headers.lastHeader(key).value(), UTF_8);
    }

    public static TopicPartition tp(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    public static <K, V> ProducerRecord<K, V> record(String topic, V value) {
        return new ProducerRecord<>(topic, value);
    }

    public static <K, V> ProducerRecord<K, V> record(String topic, K key, V value) {
        return new ProducerRecord<>(topic, key, value);
    }

    public static <K, V> ProducerRecord<K, V> record(String topic, Integer partition, K key, V value) {
        return new ProducerRecord<>(topic, partition, key, value);
    }

    public static <T> Uni<T> waitFor(Uni<T> source, Predicate<T> predicate, Duration pollDelay) {
        return source.repeat().withDelay(pollDelay)
                .whilst(t -> predicate.negate().test(t))
                .select().last().toUni();
    }

    public static Uni<Void> waitFor(BooleanSupplier predicate, Duration pollDelay) {
        return Uni.createFrom().voidItem().repeat().withDelay(pollDelay)
                .whilst(t -> !predicate.getAsBoolean())
                .select().last().toUni();
    }

    protected static <T> Uni<T> toUni(KafkaFuture<T> kafkaFuture) {
        return Uni.createFrom().completionStage(kafkaFuture.toCompletionStage());
    }

    /*
     * SERDES
     */

    public <T> void registerSerde(Class<T> type, Serde<T> serde) {
        serdeMap.put(type, serde);
    }

    public <T> void registerSerde(Class<T> type, Serializer<T> serializer, Deserializer<T> deserializer) {
        registerSerde(type, Serdes.serdeFrom(serializer, deserializer));
    }

    @SuppressWarnings({ "unchecked" })
    public <T> Serde<T> getSerdeForType(Class<T> type) {
        Serde<?> serde = serdeMap.get(type);
        if (serde != null) {
            return (Serde<T>) serde;
        }
        return Serdes.serdeFrom(type);
    }

    public TopicsCompanion topics() {
        return new TopicsCompanion(getOrCreateAdminClient(), kafkaApiTimeout);
    }

    public OffsetsCompanion offsets() {
        return new OffsetsCompanion(getOrCreateAdminClient(), kafkaApiTimeout);
    }

    public ConsumerGroupsCompanion consumerGroups() {
        return new ConsumerGroupsCompanion(getOrCreateAdminClient(), kafkaApiTimeout);
    }

    public ClusterCompanion cluster() {
        return new ClusterCompanion(getOrCreateAdminClient(), kafkaApiTimeout);
    }

    public void deleteRecords(Map<TopicPartition, RecordsToDelete> offsetsToDelete) {
        toUni(getOrCreateAdminClient().deleteRecords(offsetsToDelete).all())
                .await().atMost(kafkaApiTimeout);
    }

    public void deleteRecords(TopicPartition partition, Long beforeOffset) {
        Map<TopicPartition, RecordsToDelete> offsetsToDelete = new HashMap<>();
        offsetsToDelete.put(partition, RecordsToDelete.beforeOffset(beforeOffset));
        deleteRecords(offsetsToDelete);
    }

    /*
     * CONSUMER
     */

    public Map<String, Object> getConsumerProperties() {
        Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(GROUP_ID_CONFIG, "companion-" + UUID.randomUUID());
        config.put(CLIENT_ID_CONFIG, "companion-" + UUID.randomUUID());
        config.put(ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        config.put(AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());
        config.putAll(getCommonClientConfig());
        return config;
    }

    public <K, V> ConsumerBuilder<K, V> consumeWithDeserializers(
            Class<? extends Deserializer<K>> keyDeserType,
            Class<? extends Deserializer<V>> valueDeserType) {
        return new ConsumerBuilder<>(getConsumerProperties(), kafkaApiTimeout, keyDeserType, valueDeserType);
    }

    public <K, V> ConsumerBuilder<K, V> consumeWithDeserializers(
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return new ConsumerBuilder<>(getConsumerProperties(), kafkaApiTimeout, keyDeserializer, valueDeserializer);
    }

    public <K, V> ConsumerBuilder<K, V> consume(Serde<K> keySerde, Serde<V> valueSerde) {
        return consumeWithDeserializers(keySerde.deserializer(), valueSerde.deserializer());
    }

    public <K, V> ConsumerBuilder<K, V> consume(Class<K> keyType, Class<V> valueType) {
        return consume(getSerdeForType(keyType), getSerdeForType(valueType));
    }

    public <V> ConsumerBuilder<String, V> consume(Class<V> valueType) {
        return consume(Serdes.String(), getSerdeForType(valueType));
    }

    public ConsumerBuilder<String, String> consumeStrings() {
        return consume(Serdes.String(), Serdes.String());
    }

    public ConsumerBuilder<String, Integer> consumeIntegers() {
        return consume(Serdes.String(), Serdes.Integer());
    }

    public ConsumerBuilder<String, Double> consumeDoubles() {
        return consume(Serdes.String(), Serdes.Double());
    }

    /*
     * PRODUCER
     */

    public Map<String, Object> getProducerProperties() {
        Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "companion-" + UUID.randomUUID());
        config.putAll(getCommonClientConfig());
        return config;
    }

    public <K, V> ProducerBuilder<K, V> produceWithSerializers(
            Class<? extends Serializer<K>> keySerializerType,
            Class<? extends Serializer<V>> valueSerializerType) {
        return new ProducerBuilder<>(getProducerProperties(), kafkaApiTimeout, keySerializerType, valueSerializerType);
    }

    public <K, V> ProducerBuilder<K, V> produceWithSerializers(
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        return new ProducerBuilder<>(getProducerProperties(), kafkaApiTimeout, keySerializer, valueSerializer);
    }

    public <K, V> ProducerBuilder<K, V> produce(Serde<K> keySerde, Serde<V> valueSerde) {
        return new ProducerBuilder<>(getProducerProperties(), kafkaApiTimeout, keySerde, valueSerde);
    }

    public <K, V> ProducerBuilder<K, V> produce(Class<K> keyType, Class<V> valueType) {
        return produce(getSerdeForType(keyType), getSerdeForType(valueType));
    }

    public <V> ProducerBuilder<String, V> produce(Class<V> valueType) {
        return produce(Serdes.String(), getSerdeForType(valueType));
    }

    public ProducerBuilder<String, String> produceStrings() {
        return produce(Serdes.String(), Serdes.String());
    }

    public ProducerBuilder<String, Integer> produceIntegers() {
        return produce(Serdes.String(), Serdes.Integer());
    }

    public ProducerBuilder<String, Double> produceDoubles() {
        return produce(Serdes.String(), Serdes.Double());
    }

}
