# Dependencies (both bins and HOMEs should be part of PATH):

Apache Flume

Apache Kafka

Apache Zookeeper

Python 2.7

# Instructions

1. create the following directories:
    
    /etc/flume-ng/plugins.d/twitter-source/lib

    /etc/flume-ng/plugins.d/twitter-interceptor/libext

    /etc/flume-ng/plugins.d/twitter-interceptor/lib

2.
    Place flume/interceptor/target/interceptor-0.0.1-SNAPSHOT.jar into /etc/flume-ng/plugins.d/twitter-interceptor/lib

    Place both flume/twitter-source/target/dependency/twitter4j*.jars into /etc/flume-ng/plugins.d/twitter-interceptor/libext

    Place flume/interceptor/target/dependency/json*.jar into /etc/flume-ng/plugins.d/twitter-interceptor/libext

    Place flume/twitter-source/target/twitter-source-0.0.1-SNAPSHOT.jar into /etc/flume-ng/plugins.d/twitter-source/lib

3. Start zookeeper: 

        zkServer.sh start

4. Start kafka:

        kafka-server-start.sh $KAFKA_HOME/config/server.properties

5. Create a kafka-topic:

        kafka-topics.sh --zookeeper localhost:2181 --create --topic twitter_stream --partitions 1 --replication-factor 1

6. Run the flume agent:

        flume-ng agent --conf /etc/flume-ng/conf \
        --conf-file flume/flume_twitter_to_kafka.conf \
        --name agent1 \
        --plugins-path /etc/flume-ng/plugins.d/ \
        -Dflume.root.logger=INFO,console -Xmx1g

    If everything works correctly, you should see the interceptor filtering for English tweets in the flume window.


7. The sentiment analyser only works with Python 2.7, so ensure this is installed. It also requires specific libraries, 
    so these can be installed by running: pip install -r requirements.txt


8. Run dashboard/app.py with Python 2.7 for the web interface. This can be accessed on http://localhost:5001/

9. Run the sentiment analysis:

        spark-submit --master 'local[3]' --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.7.jar \
        spark-streaming/trending_topics_sentiment.py twitter_stream

This will feed data to the website, where you can see the analysis being done. In total, you should now have four
consoles open and a web browser to check the result. 

# Credits

Credits to http://davidiscoding.com/real-time-twitter-sentiment-analysis-pt-1-introduction 
for providing most of the code. We have fixed minor issues, like faulty flask 
argument passing and removed the need for quickstart.cloudera servers, as 
they no longer exist. 
