package io.github.andylke.demo.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
class FooTopicConfiguration {

  @Value("${spring.kafka.listener.concurrency}")
  private int partitionSize = 2;

  @Bean
  public NewTopic fooTopic() {
    return TopicBuilder.name("foo").partitions(partitionSize).build();
  }
}
