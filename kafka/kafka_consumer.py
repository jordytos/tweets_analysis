from confluent_kafka import Consumer
from twitter import Tweet
import json
from cassandra.cluster import Cluster
import numpy as np
import matplotlib.pyplot as plt
import datetime

# =========== CASSANDRA =================

cluster = Cluster(['127.0.0.1','172.18.0.4'])
session = cluster.connect('')
keyspace_name = 'tweets'

#Création de notre keyspace (s'il n'existe pas)
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '3'}}")
session.set_keyspace(keyspace_name)

#Création de nos tables (s'ils n'existent pas)

# TWEETS RAW
session.execute("""
    CREATE TABLE IF NOT EXISTS tweets_raw (
        id BIGINT PRIMARY KEY,
        user TEXT,
        tweet_date TIMESTAMP,
        tweet TEXT,
        location TEXT,
        lang TEXT
    )
""")

# TWEETS PREPRO
session.execute("""
    CREATE TABLE IF NOT EXISTS tweets_preproced (
        id BIGINT PRIMARY KEY,
        user TEXT,
        tweet_date TIMESTAMP,
        tweet TEXT,
        clean_tweet TEXT,
        polarity FLOAT,
        subjectivity FLOAT,
        sentiment TEXT
    )
""")


# INDEX ON SENTIMENT
session.execute("""
    CREATE INDEX IF NOT EXISTS tweets_sentiments ON tweets.tweets_preproced (sentiment)
""")


# TWEETS METRICS
session.execute("""
    CREATE TABLE IF NOT EXISTS tweets_metrics (
        id UUID PRIMARY KEY,
        production_model_rfc_log_loss TEXT,
        production_model_rfc_f1_score TEXT,
        production_model_rfc_accuracy_score TEXT,
        production_model_rfc_run INT
    )
""")

#========================================



#============== KAFKA ===================

# configuration de notre consumer kafka 
consumer = Consumer({'bootstrap.servers':'localhost:9092localhost:9093,localhost:9094','group.id':'tweets-consumer',
                     'auto.offset.reset':'earliest'})

print('Available topics to consume: ', consumer.list_topics().topics)

consumer.subscribe(['tweets-analysis']) # On s'abonne à ce topic Kafka

#========================================


def main():
    colName = ["-5min","-10min","-15min","-20min", "-25min","-30min","-35min","-40min","-45min", "-50min"]
    x_line = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    tabPos = np.array([0,0,0,0,0,0,0,0,0,0])
    tabNeg = np.array([0,0,0,0,0,0,0,0,0,0])
    tabTot = np.array([0,0,0,0,0,0,0,0,0,0])

    # Créer une fenêtre pour le graphique
    fig, ax = plt.subplots()

    # Ajouter les barres empilées
    bar1 = ax.bar(colName, tabNeg, label='Negatif Sentiment', color='#FF0040')
    bar2 = ax.bar(colName, tabPos, bottom=tabNeg, label='Positif Sentiement', color='#00FF80')

    # Ajouter la ligne
    ax2 = ax.twinx()
    ax2.set_ylabel("Valeurs", color='orange')
    line, = ax2.plot(x_line, tabTot, label='Total Twitter Wrapper', color='#153327')

    # Afficher le graphique
    plt.show(block=False)

    Pos = 0
    Neg = 0
    Tot = 0
    timref = datetime.datetime.now()

    while True:
        msg = consumer.poll(1.0) # timeout
        tabTot[0] = Tot
        tabPos[0] = Pos
        tabNeg[0] = Neg

        if (datetime.datetime.now() - timref).seconds >= 300 :
            timref = datetime.datetime.now()

            Pos = 0
            tabPos = np.roll(tabPos, 1)
            tabPos[0] = Pos

            Neg = 0
            tabNeg = np.roll(tabNeg, 1)
            tabNeg[0] = Neg

            Tot = 0
            tabTot = np.roll(tabTot, 1)
            tabTot[0] = Tot

            for col in range(len(colName)):
                bar1[col].set_height(tabNeg[col])
                bar2[col].set_height(tabPos[col])
                line.set_ydata(tabTot)

            # Rafraîchir le graphique
            plt.draw()
            plt.pause(1)
        
        if msg:
            tweet_data=msg.value().decode('utf-8')
            tweet_json = json.loads(tweet_data)
            my_tweet = Tweet(tweetID = str(tweet_json['id']), tweetText = str(tweet_json['tweet']))
        
        
            # On ajoute chaque données dans Cassandra
        
            # INSERT TWEET RAW
            session.execute(
                """
                INSERT INTO tweets_raw (id, user, tweet_date, tweet, location, lang)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (tweet_json['id'], tweet_json['user'], tweet_json['tweet_date'], tweet_json['tweet'], tweet_json['location'], tweet_json['lang'])
            )
            
            
            # INSERT TWEET PREPROCED
            session.execute(
                """
                INSERT INTO tweets_preproced (id, user, tweet_date, tweet, clean_tweet, polarity, subjectivity, sentiment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (int(my_tweet.id), tweet_json['user'], tweet_json['tweet_date'], my_tweet.text, my_tweet.cleanText, my_tweet.polarity, my_tweet.subjectivity, my_tweet.sentiment)
            )
            
            print(" - === - ")
            
            if my_tweet.sentiment == "positive":
                Pos += 1
                Tot += 1
                print("\033[1;32m"+str(my_tweet.output()))
            elif (my_tweet.sentiment == "negative"):
                Neg -= 1
                Tot += 1
                print("\033[1;31m"+str(my_tweet.output()))
            else :
                print("\033[1;37m"+str(my_tweet.output()))
                Tot += 1


            for col in range(len(colName)):
                bar1[col].set_height(tabNeg[col])
                bar2[col].set_height(tabPos[col])
                line.set_ydata(tabTot)

            ax.set_ylim(min(tabNeg)-10, max(tabPos)+10)
            ax2.set_ylim(0,max(tabTot)+10)
            # Rafraîchir le graphique
            plt.draw()
            plt.pause(1)



        
    # consumer.close()
    # cluster.shutdown()
    # session.shutdown()
        
if __name__ == '__main__':
    main()