package io.github.andylke.demo.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class FooProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FooProducer.class);

  @Autowired private KafkaTemplate<String, String> kafkaTemplate;

  @Scheduled(initialDelay = 500, fixedDelay = 5000)
  public void send() {
    final List<CompletableFuture<?>> futures = new ArrayList<CompletableFuture<?>>();

    final int records = 10;
    final long startTime = System.nanoTime();
    for (int index = 0; index < records; index++) {
      futures.add(kafkaTemplate.send("foo", "foo" + index).completable());
    }
    try {
      CompletableFuture.allOf(futures.toArray(size -> new CompletableFuture<?>[size])).join();
    } catch (CompletionException e) {
      LOGGER.error("Failed sending messages", e);
      throw e;
    }

    LOGGER.info(
        "Sent [{}] records, elapsed [{}]",
        records,
        Duration.ofNanos(startTime - System.nanoTime()));
  }
}
