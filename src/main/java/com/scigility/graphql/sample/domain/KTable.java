package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class KTable {
  @Getter
  @Setter
  private String id;

  @Getter
  @Setter
  private String key;

  @Getter
  @Setter
  private String value;

  @Getter
  @Setter
  private String className;
}
