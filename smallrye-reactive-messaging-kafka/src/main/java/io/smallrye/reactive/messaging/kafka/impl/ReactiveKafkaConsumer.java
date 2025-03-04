package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.fault.DeserializerWrapper;
import io.smallrye.reactive.messaging.providers.i18n.ProviderLogging;
import io.vertx.core.Context;

public class ReactiveKafkaConsumer<K, V> implements io.smallrye.reactive.messaging.kafka.KafkaConsumer<K, V> {

    private final AtomicBoolean closed = new AtomicBoolean(true);

    /**
     * Avoid concurrent call to `poll`
     */
    private final AtomicBoolean polling = new AtomicBoolean(false);
    private final KafkaSource<K, V> source;
    private final Uni<Consumer<K, V>> consumerUni;
    private final AtomicReference<Consumer<K, V>> consumerRef = new AtomicReference<>();
    private final KafkaConnectorIncomingConfiguration configuration;
    private final Duration pollTimeout;
    private ConsumerRebalanceListener rebalanceListener;

    private final AtomicBoolean paused = new AtomicBoolean();

    private final ScheduledExecutorService kafkaWorker;
    private final KafkaRecordStream<K, V> stream;
    private final KafkaRecordBatchStream<K, V> batchStream;
    private final Map<String, Object> kafkaConfiguration;

    public ReactiveKafkaConsumer(KafkaConnectorIncomingConfiguration config, KafkaSource<K, V> source,
            java.util.function.Consumer<Consumer<K, V>> onConsumerCreated) {
        this.configuration = config;
        this.source = source;
        kafkaConfiguration = getKafkaConsumerConfiguration(configuration, source.getConsumerGroup(),
                source.getConsumerIndex());

        Instance<DeserializationFailureHandler<?>> failureHandlers = source.getDeserializationFailureHandlers();
        String keyDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        String valueDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        if (valueDeserializerCN == null) {
            throw ex.missingValueDeserializer(config.getChannel(), config.getChannel());
        }

        Deserializer<K> keyDeserializer = new DeserializerWrapper<>(keyDeserializerCN, true,
                getDeserializationHandler(true, failureHandlers), source, config.getFailOnDeserializationFailure());
        Deserializer<V> valueDeserializer = new DeserializerWrapper<>(valueDeserializerCN, false,
                getDeserializationHandler(false, failureHandlers), source, config.getFailOnDeserializationFailure());

        // Configure the underlying deserializers
        keyDeserializer.configure(kafkaConfiguration, true);
        valueDeserializer.configure(kafkaConfiguration, false);

        pollTimeout = Duration.ofMillis(config.getPollTimeout());

        kafkaWorker = Executors.newSingleThreadScheduledExecutor(KafkaPollingThread::new);

        stream = new KafkaRecordStream<>(this, config, source.getContext().getDelegate());
        batchStream = new KafkaRecordBatchStream<>(this, config, source.getContext().getDelegate());
        consumerUni = Uni.createFrom().item(() -> consumerRef.updateAndGet(c -> {
            if (c != null) {
                return c;
            } else {
                KafkaConsumer<K, V> consumer = new KafkaConsumer<>(kafkaConfiguration, keyDeserializer, valueDeserializer);
                onConsumerCreated.accept(consumer);
                closed.set(false);
                return consumer;
            }
        })).memoize().until(closed::get)
                .runSubscriptionOn(kafkaWorker);
        if (!config.getLazyClient()) {
            consumerUni.await().indefinitely();
        }
    }

    public Uni<Consumer<K, V>> withConsumerOnPollingThread() {
        return consumerUni;
    }

    public void setRebalanceListener() {
        try {
            rebalanceListener = RebalanceListeners.createRebalanceListener(this, configuration, source.getConsumerGroup(),
                    source.getConsumerRebalanceListeners(), source.getCommitHandler());
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    // Visible to use for rebalance on MockConsumer which doesn't call listeners
    public ConsumerRebalanceListener getRebalanceListener() {
        return this.rebalanceListener;
    }

    @Override
    @CheckReturnValue
    public <T> Uni<T> runOnPollingThread(Function<Consumer<K, V>, T> action) {
        return withConsumerOnPollingThread().map(action);
    }

    @Override
    @CheckReturnValue
    public Uni<Void> runOnPollingThread(java.util.function.Consumer<Consumer<K, V>> action) {
        return withConsumerOnPollingThread()
                .invoke(action)
                .replaceWithVoid();
    }

    Uni<Void> executeWithDelay(Runnable action, Duration delay) {
        return Uni.createFrom().emitter(e -> {
            kafkaWorker.schedule(() -> {
                try {
                    action.run();
                } catch (Exception ex) {
                    e.fail(ex);
                    return;
                }
                e.complete(null);
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    @SuppressWarnings("unchecked")
    Uni<ConsumerRecords<K, V>> poll() {
        if (polling.compareAndSet(false, true)) {
            return runOnPollingThread(c -> {
                if (System.getSecurityManager() == null) {
                    return paused.get() ? c.poll(Duration.ZERO) : c.poll(pollTimeout);
                } else {
                    return AccessController.doPrivileged(new PrivilegedAction<ConsumerRecords<K, V>>() {
                        @Override
                        public ConsumerRecords<K, V> run() {
                            return paused.get() ? c.poll(Duration.ZERO) : c.poll(pollTimeout);
                        }
                    });
                }
            })
                    .eventually(() -> polling.set(false))
                    .onFailure(WakeupException.class).recoverWithItem((ConsumerRecords<K, V>) ConsumerRecords.EMPTY);
        } else {
            // polling already in progress
            return Uni.createFrom().item((ConsumerRecords<K, V>) ConsumerRecords.EMPTY);
        }
    }

    @Override
    @CheckReturnValue
    public Uni<Set<TopicPartition>> pause() {
        if (paused.compareAndSet(false, true)) {
            return runOnPollingThread(c -> {
                Set<TopicPartition> tps = c.assignment();
                c.pause(tps);
                return tps;
            });
        } else {
            return runOnPollingThread((Function<Consumer<K, V>, Set<TopicPartition>>) Consumer::paused);
        }
    }

    @Override
    @CheckReturnValue
    public Uni<Set<TopicPartition>> paused() {
        return runOnPollingThread((Function<Consumer<K, V>, Set<TopicPartition>>) Consumer::paused);
    }

    @Override
    @CheckReturnValue
    public Uni<Map<TopicPartition, OffsetAndMetadata>> committed(TopicPartition... tps) {
        return runOnPollingThread(c -> {
            return c.committed(new LinkedHashSet<>(Arrays.asList(tps)));
        });
    }

    @CheckReturnValue
    public Multi<ConsumerRecord<K, V>> subscribe(Set<String> topics) {
        return stream.onSubscription().call(() -> runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        }));
    }

    @CheckReturnValue
    public Multi<ConsumerRecord<K, V>> subscribe(Pattern topics) {
        return stream.onSubscription().call(() -> runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        }));
    }

    @CheckReturnValue
    Multi<ConsumerRecords<K, V>> subscribeBatch(Set<String> topics) {
        return batchStream.onSubscription().call(() -> runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        }));
    }

    @CheckReturnValue
    Multi<ConsumerRecords<K, V>> subscribeBatch(Pattern topics) {
        return batchStream.onSubscription().call(() -> runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        }));
    }

    @Override
    @CheckReturnValue
    public Uni<Void> resume() {
        if (paused.get()) {
            return runOnPollingThread(c -> {
                Set<TopicPartition> assignment = c.assignment();
                c.resume(assignment);
            }).invoke(() -> paused.set(false));
        } else {
            return Uni.createFrom().voidItem();
        }
    }

    @Override
    @CheckReturnValue
    public Uni<ConsumerGroupMetadata> consumerGroupMetadata() {
        return runOnPollingThread((Function<Consumer<K, V>, ConsumerGroupMetadata>) Consumer::groupMetadata);
    }

    @Override
    @CheckReturnValue
    public Uni<Void> resetToLastCommittedPositions() {
        return runOnPollingThread(c -> {
            Set<TopicPartition> assignments = c.assignment();
            c.pause(assignments);
            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(assignments);
            for (TopicPartition tp : assignments) {
                OffsetAndMetadata offsetAndMetadata = committed.get(tp);
                if (offsetAndMetadata != null) {
                    // Seek to next offset position
                    c.seek(tp, offsetAndMetadata.offset());
                } else {
                    // Seek to beginning of the topic partition
                    c.seekToBeginning(Collections.singleton(tp));
                }
            }
            removeFromQueueRecordsFromTopicPartitions(assignments);
            c.resume(c.assignment());
        });
    }

    private static Map<String, Object> getKafkaConsumerConfiguration(KafkaConnectorIncomingConfiguration configuration,
            String consumerGroup, int index) {
        Map<String, Object> map = new HashMap<>();
        JsonHelper.asJsonObject(configuration.config())
                .forEach(e -> map.put(e.getKey(), e.getValue().toString()));
        map.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        if (!map.containsKey(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            map.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        String servers = configuration.getBootstrapServers();
        if (!map.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!map.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
            map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, configuration.getKeyDeserializer());
        }

        if (!map.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            log.disableAutoCommit(configuration.getChannel());
            map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }

        // Consumer id generation:
        // 1. If no client id set in the config, set it to channel name, the prefix default value is "kafka-consumer-",
        // 1. If a client id set in the config, prefix with the default value "",
        // In any case if consumer index is -1, suffix is "", otherwise, suffix the index.

        String suffix = index == -1 ? ("") : ("-" + index);
        map.compute(ConsumerConfig.CLIENT_ID_CONFIG, (k, configured) -> {
            if (configured == null) {
                String prefix = configuration.getClientIdPrefix().orElse("kafka-consumer-");
                // Case 1
                return prefix + configuration.getChannel() + suffix;
            } else {
                String prefix = configuration.getClientIdPrefix().orElse("");
                // Case 2
                return prefix + configured + suffix;
            }
        });

        ConfigurationCleaner.cleanupConsumerConfiguration(map);

        return map;
    }

    private <T> DeserializationFailureHandler<T> getDeserializationHandler(boolean isKey,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers) {
        return createDeserializationFailureHandler(isKey, deserializationFailureHandlers, configuration);

    }

    @SuppressWarnings({ "unchecked" })
    private static <T> DeserializationFailureHandler<T> createDeserializationFailureHandler(boolean isKey,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            KafkaConnectorIncomingConfiguration configuration) {
        String name = isKey ? configuration.getKeyDeserializationFailureHandler().orElse(null)
                : configuration.getValueDeserializationFailureHandler().orElse(null);

        if (name == null) {
            return null;
        }

        Instance<DeserializationFailureHandler<?>> matching = deserializationFailureHandlers
                .select(Identifier.Literal.of(name));
        if (matching.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            matching = deserializationFailureHandlers.select(NamedLiteral.of(name));
            if (!matching.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }

        if (matching.isUnsatisfied()) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel());
        } else if (matching.stream().count() > 1) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel(),
                    (int) matching.stream().count());
        } else if (matching.stream().count() == 1) {
            return (DeserializationFailureHandler<T>) matching.get();
        } else {
            return null;
        }
    }

    public String get(String attribute) {
        return (String) kafkaConfiguration.get(attribute);
    }

    @Override
    public Consumer<K, V> unwrap() {
        return consumerRef.get();
    }

    @Override
    @CheckReturnValue
    public Uni<Void> commit(Map<TopicPartition, OffsetAndMetadata> map) {
        return runOnPollingThread(c -> {
            c.commitSync(map);
        });
    }

    @CheckReturnValue
    @Override
    public Uni<Void> commitAsync(Map<TopicPartition, OffsetAndMetadata> map) {
        return withConsumerOnPollingThread().chain(c -> Uni.createFrom().emitter(e -> {
            c.commitAsync(map, (offsets, exception) -> {
                if (exception != null) {
                    e.fail(exception);
                } else {
                    e.complete(null);
                }
            });
        }));
    }

    @Override
    public Map<String, ?> configuration() {
        return kafkaConfiguration;
    }

    public void close() {
        int timeout = configuration.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class).orElse(1000);
        if (closed.compareAndSet(false, true)) {
            Uni<Void> uni = runOnPollingThread(c -> {
                if (System.getSecurityManager() == null) {
                    c.close(Duration.ofMillis(timeout));
                } else {
                    AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                        c.close(Duration.ofMillis(timeout));
                        return null;
                    });
                }
            }).onItem().invoke(kafkaWorker::shutdown);

            // Interrupt polling
            Consumer<K, V> consumer = consumerRef.get();
            if (consumer != null) {
                consumer.wakeup();
            }
            if (Context.isOnEventLoopThread()) {
                // We can't block, just forget the result
                uni.subscribeAsCompletionStage();
            } else {
                uni.await().atMost(Duration.ofMillis(timeout * 2L));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void injectClient(MockConsumer<?, ?> consumer) {
        this.consumerRef.getAndUpdate(previous -> {
            if (previous != null) {
                previous.close();
            }
            closed.set(true);
            return (Consumer<K, V>) consumer;
        });
        // force uni to re-memoize
        this.consumerUni.await().indefinitely();
        closed.set(false);
    }

    @Override
    @CheckReturnValue
    public Uni<Map<TopicPartition, Long>> getPositions() {
        return runOnPollingThread(c -> {
            Map<TopicPartition, Long> map = new HashMap<>();
            c.assignment()
                    .forEach(tp -> map.put(tp, c.position(tp)));
            return map;
        });
    }

    @Override
    @CheckReturnValue
    public Uni<Set<TopicPartition>> getAssignments() {
        return runOnPollingThread((Function<Consumer<K, V>, Set<TopicPartition>>) Consumer::assignment);
    }

    @Override
    @CheckReturnValue
    public Uni<Void> seek(TopicPartition partition, long offset) {
        return runOnPollingThread(c -> {
            c.seek(partition, offset);
        });
    }

    @Override
    @CheckReturnValue
    public Uni<Void> seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        return runOnPollingThread(c -> {
            c.seek(partition, offsetAndMetadata);
        });
    }

    @Override
    @CheckReturnValue
    public Uni<Void> seekToBeginning(Collection<TopicPartition> partitions) {
        return runOnPollingThread(c -> {
            c.seekToBeginning(partitions);
        });
    }

    @Override
    @CheckReturnValue
    public Uni<Void> seekToEnd(Collection<TopicPartition> partitions) {
        return runOnPollingThread(c -> {
            c.seekToEnd(partitions);
        });
    }

    boolean isClosed() {
        return closed.get();
    }

    boolean isPaused() {
        return paused.get();
    }

    void removeFromQueueRecordsFromTopicPartitions(Collection<TopicPartition> revokedPartitions) {
        this.stream.removeFromQueueRecordsFromTopicPartitions(revokedPartitions);
        this.batchStream.removeFromQueueRecordsFromTopicPartitions(revokedPartitions);
    }
}
