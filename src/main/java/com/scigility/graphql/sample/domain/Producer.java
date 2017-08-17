package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class Producer {

      @Getter
      @Setter
      private String id;

      @Getter
      @Setter
      private String topic_output;

      @Getter
      @Setter
      private String className;
}
