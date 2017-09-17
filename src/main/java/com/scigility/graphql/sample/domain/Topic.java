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
public class Topic {
    @Getter
    @Setter
    private String name;

    @Getter
    @Setter
    private String schema;

}
