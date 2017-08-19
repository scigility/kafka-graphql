package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.ArrayList;
import java.util.Collections;
@AllArgsConstructor
@NoArgsConstructor
public class Kafka {
  @Getter
  @Setter
  private Integer id;

  @Getter
  @Setter
  private String broker;

  @Getter
  @Setter
  private String zookeeper;

  private List<Topic> topics = new ArrayList<>();

  public List<Topic> getTopics(){
      return Collections.unmodifiableList(this.topics);
  }

  public void addToTopics(final Topic topic){
      this.topics.add(topic);
  }

  //private List<Consumer> consumers;

  //private List<Producer> producers;

  //private List<KStream> KStreams;

  //private List<KTable> KTables;
}
