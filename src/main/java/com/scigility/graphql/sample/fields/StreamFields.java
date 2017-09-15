package com.scigility.graphql.sample.fields;

import com.merapar.graphql.GraphQlFields;
import com.scigility.graphql.sample.dataFetchers.StreamDataFetcher;
import com.scigility.graphql.sample.dataFetchers.TopicDataFetcher;
import graphql.Scalars;
import graphql.schema.*;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.merapar.graphql.base.GraphQlFieldsHelper.*;
import static graphql.Scalars.*;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLObjectType.newObject;

@Component
public class StreamFields implements GraphQlFields {

    @Autowired
    private StreamDataFetcher streamDataFetcher;

    @Getter
    private GraphQLObjectType streamType;

    @Getter
    private GraphQLObjectType tableRecordType;

    private GraphQLInputObjectType streamStartInputType;
    private GraphQLInputObjectType streamStopInputType;
    private GraphQLInputObjectType filterStreamInputType;

    private GraphQLFieldDefinition streamField;

    private GraphQLFieldDefinition streamStartField;
    private GraphQLFieldDefinition streamStopField;

    @Getter
    private List<GraphQLFieldDefinition> queryFields;

    @Getter
    private List<GraphQLFieldDefinition> mutationFields;

    @PostConstruct
    public void postConstruct() {
        createTypes();
        createFields();
        queryFields = Collections.singletonList(streamField);
        mutationFields = Arrays.asList( streamStartField, streamStopField);
    }

    private void createTypes() {
        streamType = newObject().name("stream").description("A stream record")
                .field(newFieldDefinition().name("name").description("The key").type(GraphQLString).build())
                .field(newFieldDefinition().name("in").description("The key").type(GraphQLString).build())
                .field(newFieldDefinition().name("out").description("The value").type(GraphQLString).build())
                .field(newFieldDefinition().name("status").description("The offset").type(GraphQLLong).build())
                .field(newFieldDefinition().name("table").description("The partition").type(GraphQLInt).build())
                .build();

        tableRecordType = newObject().name("tableRecord").description("A table Record")
                .field(newFieldDefinition().name("key").description("key").type(GraphQLString).build())
                .field(newFieldDefinition().name("value").description("value").type(GraphQLString).build())
                .build();

        streamStartInputType = newInputObject().name("streamStart").description("A fields")
                .field(newInputObjectField().name("in").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .field(newInputObjectField().name("out").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();

        streamStopInputType = newInputObject().name("streamStop").description("A fields")
                .field(newInputObjectField().name("name").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();

        filterStreamInputType = newInputObject().name("filterStreamInput")
                .field(newInputObjectField().name("name").type(GraphQLInt).build())
                .build();

    }

    private void createFields() {
        streamField = newFieldDefinition()
                .name("stream").description("Provide an overview of all topics")
                .type(new GraphQLList(tableRecordType))
                .argument(newArgument().name(FILTER).type(filterStreamInputType).build())
                .dataFetcher(environment -> streamDataFetcher.getStreamByFilter(getFilterMap(environment)))
                .build();

        streamStartField = newFieldDefinition()
                .name("streamStart").description("Start the stream")
                .type(new GraphQLList(tableRecordType))
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(streamStartInputType)).build())
                .dataFetcher(environment -> streamDataFetcher.streamStart(getInputMap(environment)))
                .build();

        streamStopField = newFieldDefinition()
                .name("streamStop").description("Stop the stream")
                .type(streamType)
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(streamStopInputType)).build())
                .dataFetcher(environment -> streamDataFetcher.streamStop(getInputMap(environment)))
                .build();
    }
}
