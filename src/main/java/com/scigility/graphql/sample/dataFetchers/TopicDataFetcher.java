package com.scigility.graphql.sample.dataFetchers;

import com.merapar.graphql.base.TypedValueMap;
import com.scigility.graphql.sample.domain.Topic;
import lombok.val;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.*;

@Component
public class TopicDataFetcher {
    private Log log = LogFactory.getLog(TopicDataFetcher.class);
    
    public Map<Integer, Topic> topics = new HashMap<>();

    public List<Topic> getTopicsByFilter(TypedValueMap arguments) {
      log.info("getTopicsByFilter");
        Integer id = arguments.get("id");

        if (id != null) {
            return Collections.singletonList(topics.get(id));
        } else {
            return new ArrayList<>(topics.values());
        }
    }

    public Topic addTopic(TypedValueMap arguments) {
      log.info("addTopic");
        val topic = new Topic();

        topic.setId(arguments.get("id"));
        topic.setName(arguments.get("name"));

        topics.put(topic.getId(), topic);

        return topic;
    }

    public Topic updateTopic(TypedValueMap arguments) {
      log.info("updateTopic");
        val topic = topics.get(arguments.get("id"));

        if (arguments.containsKey("name")) {
            topic.setName(arguments.get("name"));
        }

        return topic;
    }

    public Topic deleteTopic(TypedValueMap arguments) {
      log.info("deleteTopic");
        val topic = topics.get(arguments.get("id"));

        topics.remove(topic.getId());

        return topic;
    }
}
