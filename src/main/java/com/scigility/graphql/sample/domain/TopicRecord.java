package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
public class TopicRecord {
    @Getter
    @Setter
    private String key;

    @Getter
    @Setter
    private String value;

    @Getter
    @Setter
    private long offset;

    @Getter
    @Setter
    private int partition;
}
