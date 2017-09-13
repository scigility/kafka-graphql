package com.scigility.graphql.sample.fields;

import com.merapar.graphql.GraphQlFields;
import com.scigility.graphql.sample.dataFetchers.KafkaDataFetcher;
import com.scigility.graphql.sample.domain.Kafka;
import graphql.Scalars;
import graphql.schema.*;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static com.merapar.graphql.base.GraphQlFieldsHelper.*;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLObjectType.newObject;

@Component
public class KafkaFields implements GraphQlFields {

    @Autowired
    private KafkaDataFetcher kafkaDataFetcher;

    @Autowired
    private TopicFields topicFields;

    private Log log = LogFactory.getLog(KafkaFields.class);

    private GraphQLObjectType kafkaType;

    private GraphQLInputObjectType addKafkaTopicInputType;
    private GraphQLInputObjectType updateKafkaInputType;

    private GraphQLInputObjectType filterKafkaInputType;

    private GraphQLFieldDefinition kafkasField;
    private GraphQLFieldDefinition addKafkaTopicField;
    private GraphQLFieldDefinition updateKafkaField;

    @Getter
    private List<GraphQLFieldDefinition> queryFields;

    @Getter
    private List<GraphQLFieldDefinition> mutationFields;

    @PostConstruct
    public void postConstruct() {
        createTypes();
        createFields();
        queryFields = Collections.singletonList(kafkasField);
        mutationFields = Arrays.asList(addKafkaTopicField, updateKafkaField);
    }

    private void createTypes() {
        log.info("createTypes");
        kafkaType = newObject().name("kafka").description("A kafka connection")
                .field(newFieldDefinition().name("broker").description("The broker").type(GraphQLString).build())
                .field(newFieldDefinition().name("zookeeper").description("The zookeeper").type(GraphQLString).build())
                .field(newFieldDefinition().name("topics").description("The topics inside kafka").type(new GraphQLList(topicFields.getTopicType()))
                        .dataFetcher(environment -> kafkaDataFetcher.getTopics((Kafka) environment.getSource()))
                        .build())
                .build();

        addKafkaTopicInputType = newInputObject().name("addKafkaTopicInput")
                .field(newInputObjectField().name("topic_name").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();

        updateKafkaInputType = newInputObject().name("updateKafkaInput")
                .field(newInputObjectField().name("broker").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .field(newInputObjectField().name("zookeeper").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();

        filterKafkaInputType = newInputObject().name("filterKafkaInput")
                .field(newInputObjectField().name("broker").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
                .build();
    }

    private void createFields() {
        log.info("createFields");
        kafkasField = newFieldDefinition()
                .name("kafka").description("Provide an overview of all kafka connection")
                .type(kafkaType)
                .argument(newArgument().name(FILTER).type(filterKafkaInputType).build())
                .dataFetcher(environment -> kafkaDataFetcher.getKafkasByFilter(getFilterMap(environment)))
                .build();

        addKafkaTopicField = newFieldDefinition()
                .name("addKafkaTopic").description("Add new topic")
                .type(kafkaType)
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(addKafkaTopicInputType)).build())
                .dataFetcher(environment -> kafkaDataFetcher.addKafkaTopicField(getInputMap(environment)))
                .build();

        updateKafkaField = newFieldDefinition()
                .name("updateKafka").description("Update existing kafka")
                .type(kafkaType)
                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(updateKafkaInputType)).build())
                .dataFetcher(environment -> kafkaDataFetcher.updateKafka(getInputMap(environment)))
                .build();
    }
}
