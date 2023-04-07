from confluent_kafka import Producer

import json
import snscrape.modules.twitter as sntwitter
import pandas as pd

import time
import logging


# logs file set up
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Définir les paramètres de connexion à Kafka
kafka_conf = {'bootstrap.servers': "localhost:9092"}
kafka_topic = 'tweets-analysis'

# Création de notre producer
producer = Producer(kafka_conf)


#####################

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(msg.value().decode('utf-8'))
        
#####################
print('Kafka Producer has been initiated...')


# votre requête de recherche twitter ici les tweets anglais sur la coupe du monde
query = "Microsoft lang:en" 

def main():
    
    tweets = {}
    
    timer = 1 * 60
    
    verif_id = []
    
    while True:
        
<<<<<<< HEAD
        # time.sleep(2)
=======
        time.sleep(2)
>>>>>>> 03b91adf19357241587207a0eeb3b0d4161172d6
        
        start_time = time.time()
        
        # On scrape nos tweets avec snscrape et les envoies dans notre topic kafka
        for tweet in sntwitter.TwitterSearchScraper(query).get_items():
            
            print("\n")
            if ((time.time() - start_time) < timer):
                if(tweet.id not in verif_id):
                    verif_id.append(tweet.id)  
                    # on ajoute les données de chaque tweet dans un dictionnaire
                    tweets = {'id': tweet.id, 'user':tweet.user.username, 'tweet_date':str(tweet.date),'tweet': tweet.content, 'location':tweet.user.location, 'lang' : tweet.lang}
                    
                    m = json.dumps(tweets,ensure_ascii=False).encode('utf-8') # on serialise en Json
                
                    producer.poll(1)
                    producer.produce('tweets-analysis',m,callback=receipt) # on envoie nos tweets dans le Topic Kafka 'tweets-analysis'
                    
                    # Attendre que tous les messages soient envoyés
                    producer.flush()   
                    
<<<<<<< HEAD
                    # time.sleep(1) # intervalle entre chaque envoie
                else :
                    print("=============================== already known")
                    break
            else:
                print(f"Délai de {timer/60} minutes dépassé, relance du producer ...")
                break
                
=======
                    time.sleep(1) # intervalle entre chaque envoie
            else:
                print(f"Délai de {timer/60} minutes dépassé, relance du producer ...")
                break
>>>>>>> 03b91adf19357241587207a0eeb3b0d4161172d6
              
      
if __name__ == '__main__':
    main()