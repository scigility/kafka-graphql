package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class Document {

    @Getter
    @Setter
    private String topic;

    @Getter
    @Setter
    private String topic_output;

    @Getter
    @Setter
    private String className;
}
