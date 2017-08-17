package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
public class TopicMessage {
    @Getter
    @Setter
    private Integer id;

    @Getter
    @Setter
    private String key;

    @Getter
    @Setter
    private String value;
}
