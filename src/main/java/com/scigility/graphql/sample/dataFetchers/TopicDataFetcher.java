package com.scigility.graphql.sample.dataFetchers;

import com.merapar.graphql.base.TypedValueMap;
import com.scigility.graphql.sample.domain.Topic;
import lombok.val;
import org.springframework.stereotype.Component;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
@Component
public class TopicDataFetcher {
    private Log log = LogFactory.getLog(TopicDataFetcher.class);

    public Map<Integer, Topic> topics = new HashMap<>();

    public List<Topic> getTopicsByFilter(TypedValueMap arguments) {
      log.info("getTopicsByFilter");

      Integer id = arguments.get("id");

      HttpClient client = new DefaultHttpClient();
      HttpGet request = new HttpGet("http://localhost:8082/topics");
      ArrayList<Topic> topicsList = new ArrayList<>();
      try{
        HttpResponse response = client.execute(request);
        BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
        String line = rd.readLine();
        JSONArray temp = JSONArray.fromObject(line);
        int length = temp.size();
        if (length > 0) {
            for (int i = 0; i < length; i++) {
              Topic topic = new Topic();
              topic.setName(temp.getString(i));
              topicsList.add(topic);
            }
        }
      } catch (java.io.IOException e){}
      return Collections.unmodifiableList(topicsList);
    }

    public Topic addTopic(TypedValueMap arguments) {
      log.info("addTopic");

      // Integer id = arguments.get("name");
      //
      // Process pr = rt.exec("kafka-topics --create --zookeeper "+ zookeeperHosts +
      //   " --replication-factor "+noOfReplication+
      //   " --partitions "+noOfPartitions+
      //   " --topic "+topicName
      //   );
      return null;
    }

    public Topic addTopicMessage(TypedValueMap arguments) {
      log.info("addTopicMessage");

      String name = arguments.get("name");
      String message = arguments.get("message");

      HttpClient client = new DefaultHttpClient();
      HttpPost post = new HttpPost("http://localhost:8082/topics/"+name);

      // curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
      //           --data '{"records":[{"value":{"name": "testUser"}}]}' \
      //           "http://localhost:8082/topics/jsontest"

      //HttpGet request = new HttpGet("http://localhost:8082/topics/"+name);
      ArrayList<Topic> topicsList = new ArrayList<>();
      try{
        StringEntity input = new StringEntity(message);
        input.setContentType("application/vnd.kafka.binary.v2+json");
        post.setEntity(input);
        HttpResponse response = client.execute(post);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String line = "";
        while ((line = rd.readLine()) != null) {
         System.out.println(line);
        }
      }catch (Exception e){}
      return null;
    }

    public Topic updateTopic(TypedValueMap arguments) {
      log.info("updateTopic");
        val topic = topics.get(arguments.get("id"));

        if (arguments.containsKey("name")) {
            topic.setName(arguments.get("name"));
        }

        return topic;
    }

    //TODO
    public Topic deleteTopic(TypedValueMap arguments) {
      log.info("deleteTopic");
        val topic = topics.get(arguments.get("id"));

        //topics.remove(topic.getId());

        return topic;
    }
}
