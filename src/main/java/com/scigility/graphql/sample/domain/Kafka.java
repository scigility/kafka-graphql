package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  private List<Topic> topics;

  //rivate List<Consumer> consumers;

  //private List<Producer> producers;

  //private List<KStream> KStreams;

  //private List<KTable> KTables;
}
