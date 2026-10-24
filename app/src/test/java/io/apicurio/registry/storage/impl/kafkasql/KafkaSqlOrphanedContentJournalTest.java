package io.apicurio.registry.storage.impl.kafkasql;

import io.apicurio.registry.AbstractResourceTestBase;
import io.apicurio.registry.storage.impl.kafkasql.messages.DeleteAllOrphanedContent0Message;
import io.apicurio.registry.utils.tests.ApicurioTestTags;
import io.apicurio.registry.utils.tests.KafkasqlTestProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Verifies that orphaned content cleanup operations are written to the KafkaSQL journal,
 * ensuring all replicas apply the deletion at the same journal offset to prevent Content ID divergence (#10700).
 */
@QuarkusTest
@TestProfile(KafkasqlTestProfile.class)
@Tag(ApicurioTestTags.SLOW)
public class KafkaSqlOrphanedContentJournalTest extends AbstractResourceTestBase {

    private static final String JOURNAL_TOPIC = "kafkasql-journal";

    @Inject
    KafkaSqlRegistryStorage kafkaSqlRegistryStorage;

    @Test
    public void testDeleteAllOrphanedContentIsJournaled() throws Exception {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        System.getProperty("bootstrap.servers.external"),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer());
        try {
            consumer.subscribe(List.of(JOURNAL_TOPIC));

            kafkaSqlRegistryStorage.deleteAllOrphanedContent();

            long deadline = System.currentTimeMillis() + 15000;
            boolean found = false;
            while (System.currentTimeMillis() < deadline && !found) {
                for (ConsumerRecord<String, String> journalRecord : consumer.poll(Duration.ofMillis(500))) {
                    if (DeleteAllOrphanedContent0Message.class.getSimpleName().equals(messageType(journalRecord))) {
                        found = true;
                        break;
                    }
                }
            }

            Assertions.assertTrue(found, "Expected a DeleteAllOrphanedContent0Message to be produced to the KafkaSQL journal topic.");
        } finally {
            consumer.close();
        }
    }

    private String messageType(ConsumerRecord<String, String> journalRecord) {
        Header header = journalRecord.headers().lastHeader(KafkaSqlSubmitter.MESSAGE_TYPE_HEADER);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
