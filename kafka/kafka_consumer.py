from confluent_kafka import Consumer
from twitter import Tweet
import json
from cassandra.cluster import Cluster


# =========== CASSANDRA =================

# cluster = Cluster(['cassandra'])
# session = cluster.connect('cassandra01')
# keyspace_name = 'tweets'
# session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '3'}}")
# session.set_keyspace(keyspace_name)

#========================================



#============== KAFKA ===================

# configuration de notre consumer kafka 
consumer = Consumer({'bootstrap.servers':"localhost:9092",'group.id':'tweets-consumer',
                     'auto.offset.reset':'earliest'})

print('Available topics to consume: ', consumer.list_topics().topics)

consumer.subscribe(['tweets-analysis']) # On s'abonne à ce topic Kafka

#========================================


def main():
    while True:
        msg = consumer.poll(1.0) # timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        tweet_data=msg.value().decode('utf-8')
        
        tweet_json = json.loads(tweet_data)
        
        my_tweet = Tweet(tweetID = str(tweet_json['id']), tweetText = str(tweet_json['tweet']))

        
        # On ajoute chaque données dans Cassandra
        #session.execute(f"INSERT INTO tweets (id, user, tweet_date, tweet, tweet_prepocess) VALUES ('{tweet_data['id']}', '{tweet_data['user']}', '{tweet_data['tweet_date']}', '{tweet_data['tweet']}', '{tweet_data['tweet_prepocess']}')")
        
        if my_tweet.sentiment == "positive":
            print("\033[1;32m"+str(my_tweet.output()))
        elif (my_tweet.sentiment == "negative"):
            print("\033[1;31m"+str(my_tweet.output()))
        else :
            print("\033[1;37m"+str(my_tweet.output()))
        
    #consumer.close()
    #cluster.shutdown()
    #session.shutdown()
        
if __name__ == '__main__':
    main()