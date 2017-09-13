package com.scigility.graphql.sample.fields;

import com.merapar.graphql.GraphQlFields;
import com.scigility.graphql.sample.dataFetchers.UserDataFetcher;
import com.scigility.graphql.sample.dataFetchers.DocumentFetcher;
import com.scigility.graphql.sample.domain.User;
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

@Component
public class DocumentFields implements GraphQlFields {

    @Autowired
    private DocumentFetcher documentFetcher;

    @Autowired
    private TopicFields documentFields;

    private GraphQLObjectType documentType;

    private GraphQLInputObjectType insertOneType;
    private GraphQLInputObjectType updateDocumentInputType;
    private GraphQLInputObjectType deleteDocumentInputType;

    private GraphQLInputObjectType filterDocumentInputType;

    private GraphQLFieldDefinition documentField;
    private GraphQLFieldDefinition insertOneField;
    private GraphQLFieldDefinition findField;
    private GraphQLFieldDefinition updateOneField;
    private GraphQLFieldDefinition deleteOneField;

    @Getter
    private List<GraphQLFieldDefinition> queryFields;

    @Getter
    private List<GraphQLFieldDefinition> mutationFields;

    @PostConstruct
    public void postConstruct() {
//        createTypes();
//        createFields();
//        queryFields = Collections.singletonList(documentField);
//        mutationFields = Arrays.asList(insertOneField, findField,
//                updateOneField, deleteOneField);
    }

    private void createTypes() {
//        documentType = newObject().name("user").description("A user")
//                .field(newFieldDefinition().name("id").description("The id").type(GraphQLInt).build())
//                .field(newFieldDefinition().name("name").description("The name").type(GraphQLString).build())
//                .build();
//
//        insertOneType = newInputObject().name("addUserInput")
//                .field(newInputObjectField().name("id").type(new GraphQLNonNull(GraphQLInt)).build())
//                .field(newInputObjectField().name("name").type(new GraphQLNonNull(Scalars.GraphQLString)).build())
//                .build();
//
//        updateDocumentInputType = newInputObject().name("updateUserInput")
//                .field(newInputObjectField().name("id").type(new GraphQLNonNull(GraphQLInt)).build())
//                .field(newInputObjectField().name("name").type(GraphQLString).build())
//                .build();
//
//        deleteDocumentInputType = newInputObject().name("deleteUserInput")
//                .field(newInputObjectField().name("id").type(new GraphQLNonNull(GraphQLInt)).build())
//                .build();
//
//        filterDocumentInputType = newInputObject().name("filterUserInput")
//                .field(newInputObjectField().name("id").type(GraphQLInt).build())
//                .build();
    }

    private void createFields() {
//        documentField = newFieldDefinition()
//                .name("users").description("Provide an overview of all users")
//                .type(new GraphQLList(documentType))
//                .argument(newArgument().name(FILTER).type(filterDocumentInputType).build())
//                .dataFetcher(environment -> documentFetcher.getDocumentByFilter(getFilterMap(environment)))
//                .build();
//
//        insertOneField = newFieldDefinition()
//                .name("addUser").description("Add new user")
//                .type(documentType)
//                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(filterDocumentInputType)).build())
//                .dataFetcher(environment -> documentFetcher.addDocument(getInputMap(environment)))
//                .build();
//
//        findField = newFieldDefinition()
//                .name("updateUser").description("Update existing user")
//                .type(documentType)
//                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(filterDocumentInputType)).build())
//                .dataFetcher(environment -> documentFetcher.updateDocument(getInputMap(environment)))
//                .build();
//
//        updateOneField = newFieldDefinition()
//                .name("deleteUser").description("Delete existing user")
//                .type(documentType)
//                .argument(newArgument().name(INPUT).type(new GraphQLNonNull(filterDocumentInputType)).build())
//                .dataFetcher(environment -> documentFetcher.deleteDocument(getInputMap(environment)))
//                .build();
    }
}
