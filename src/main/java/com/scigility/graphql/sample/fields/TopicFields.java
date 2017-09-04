package com.scigility.graphql.sample.fields;

import com.merapar.graphql.GraphQlFields;
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
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLObjectType.newObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
@Component
public class TopicFields implements GraphQlFields {

    @Autowired
    private TopicDataFetcher topicDataFetcher;

    @Getter
    private GraphQLObjectType topicType;

    private GraphQLInputObjectType addTopicInputType;
    private GraphQLInputObjectType addTopicMessageInputType;
    private GraphQLInputObjectType updateTopicInputType;
    private GraphQLInputObjectType deleteTopicInputType;

    private GraphQLInputObjectType filterTopicInputType;

    private GraphQLFieldDefinition topicsField;
    private GraphQLFieldDefinition addTopicField;
    private GraphQLFieldDefinition addTopicMessageField;
    private GraphQLFieldDefinition updateTopicField;
    private GraphQLFieldDefinition deleteTopicField;

    @Getter
    private List<GraphQLFieldDefinition> queryFields;

    @Getter
    private List<GraphQLFieldDefinition> mutationFields;

    @PostConstruct
    public void postConstruct() {
        createTypes();
        createFields();
        queryFields = Collections.singletonList(topicsField);
        mutationFields = Arrays.asList(addTopicMessageField,addTopicField, updateTopicField, deleteTopicField);
    }

    private void createTypes() {
        topicType = newObject().name("topic").description("A topic")
                .field(newFieldDefinition().name("name").description("The name").type(GraphQLString).build())
                .build();

        addTopicInputType = newInputObject().name("addTopicInput")
                .field(newInputObjectField().name("name").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();

        addTopicMessageInputType = newInputObject().name("addTopicMessageInput")
                .field(newInputObjectField().name("name").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .field(newInputObjectField().name("message").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();

        updateTopicInputType = newInputObject().name("updateTopicInput")
                .field(newInputObjectField().name("name").type(GraphQLString).build())
                .build();

        deleteTopicInputType = newInputObject().name("deleteTopicInput")
                .field(newInputObjectField().name("name").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();

        filterTopicInputType = newInputObject().name("filterTopicInput")
                .field(newInputObjectField().name("name").type(GraphQLInt).build())
                .build();
    }

    private void createFields() {
        topicsField = newFieldDefinition()
                .name("topics").description("Provide an overview of all topics")
                .type(new GraphQLList(topicType))
                .argument(newArgument().name(FILTER).type(filterTopicInputType).build())
                .dataFetcher(environment -> topicDataFetcher.getTopicsByFilter(getFilterMap(environment)))
                .build();

        addTopicField = newFieldDefinition()
                .name("addTopic").description("Add new topic")
                .type(topicType)
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(addTopicInputType)).build())
                .dataFetcher(environment -> topicDataFetcher.addTopic(getInputMap(environment)))
                .build();

        addTopicMessageField = newFieldDefinition()
                .name("addTopicMessage").description("Send a message to a topic")
                .type(topicType)
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(addTopicMessageInputType)).build())
                .dataFetcher(environment -> topicDataFetcher.addTopicMessage(getInputMap(environment)))
                .build();

        updateTopicField = newFieldDefinition()
                .name("updateTopic").description("Update existing topic")
                .type(topicType)
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(updateTopicInputType)).build())
                .dataFetcher(environment -> topicDataFetcher.updateTopic(getInputMap(environment)))
                .build();

        deleteTopicField = newFieldDefinition()
                .name("deleteTopic").description("Delete existing topic")
                .type(topicType)
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(deleteTopicInputType)).build())
                .dataFetcher(environment -> topicDataFetcher.deleteTopic(getInputMap(environment)))
                .build();
    }
}
