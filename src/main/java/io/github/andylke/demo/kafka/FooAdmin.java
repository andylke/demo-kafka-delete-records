package io.github.andylke.demo.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FooAdmin {

  private static final Logger LOGGER = LoggerFactory.getLogger(FooAdmin.class);

  @Autowired private ConsumerFactory<String, String> consumerFactory;

  @Autowired private KafkaAdmin admin;

  @Scheduled(initialDelay = 1000, fixedDelay = 5000)
  public void delete() {

    final KafkaConsumer<String, String> consumer =
        (KafkaConsumer<String, String>) consumerFactory.createConsumer();
    final List<PartitionInfo> partitionInfos = consumer.partitionsFor("foo");
    LOGGER.info("partitions for 'foo' = {}", partitionInfos);

    final List<TopicPartition> partitions = new ArrayList<TopicPartition>();
    for (PartitionInfo partitionInfo : partitionInfos) {
      partitions.add(new TopicPartition("foo", partitionInfo.partition()));
    }

    final AdminClient adminClient = AdminClient.create(admin.getConfigurationProperties());

    consumer.assign(partitions);
    final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
    endOffsets.forEach(
        (partition, endOffset) -> {
          try {
            adminClient
                .deleteRecords(Map.of(partition, RecordsToDelete.beforeOffset(endOffset)))
                .all()
                .get();
            LOGGER.info("Delete records before offset [{}] for partition {}", endOffset, partition);
          } catch (Exception e) {
            LOGGER.error("Failed to delete records", e);
          }
        });

    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
    consumer.commitAsync();

    LOGGER.info("Polled [{}] records", records.count());

    consumer.close();
  }
}
