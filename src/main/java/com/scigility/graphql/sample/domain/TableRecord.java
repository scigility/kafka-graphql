package com.scigility.graphql.sample.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
public class TableRecord {
    @Getter
    @Setter
    private String key;

    @Getter
    @Setter
    private Long value;
}
