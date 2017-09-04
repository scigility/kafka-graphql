package com.scigility.graphql.sample.domain;

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

public class Kafka {
  //defaut scenario
  //broker: "localhost:9092,localhost:9093"
  //zookeeper: "localhost:2181,localhost:2888"

  private static Kafka _instance = null;

  protected Kafka() {
    broker = "localhost:9092,localhost:9093";
    zookeeper = "localhost:2181,localhost:2888";
  }

  public static Kafka getInstance() {
    if(_instance == null) {
      _instance = new Kafka();
    }
    return _instance;
  }

  @Getter
  @Setter
  private String broker;

  @Getter
  @Setter
  private String zookeeper;

  private List<Topic> topics = new ArrayList<>();

  public List<Topic> getTopics(){
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet("http://localhost:8082/topics");
    try{
      HttpResponse response = client.execute(request);
      BufferedReader rd = new BufferedReader (new InputStreamReader(response.getEntity().getContent()));
      String line = rd.readLine();
      JSONArray temp = JSONArray.fromObject(line);
      this.topics = new ArrayList<>();
      int length = temp.size();
      if (length > 0) {
          for (int i = 0; i < length; i++) {
            Topic topic = new Topic();
            topic.setId(i);
            topic.setName(temp.getString(i));
            this.topics.add(topic);
          }
      }
    } catch (java.io.IOException e){}
    return Collections.unmodifiableList(this.topics);
  }

  public void addToTopics(final Topic topic){
      this.topics.add(topic);
  }

  //private List<Consumer> consumers;

  //private List<Producer> producers;

  //private List<KStream> KStreams;

  //private List<KTable> KTables;
}
