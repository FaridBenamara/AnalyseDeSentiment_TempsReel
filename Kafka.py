from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from textblob import TextBlob
import mysql.connector
import re
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import emoji
import psycopg2
import nltk

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')


# Définition de la fonction de nettoyage
def clean_tweet(tweet):
    # Convertir en minuscules
    tweet = tweet.lower()
    # Supprimer les URL
    tweet = re.sub(r'http\S+', '', tweet)
    # Supprimer les mentions
    tweet = re.sub(r'@\w+', '', tweet)
    # Supprimer la ponctuation
    tweet = re.sub(r'[^\w\s]', '', tweet)
    # Supprimer les contractions
    # Supprimer les caractères uniques
    tweet = re.sub(r'\s+[a-zA-Z]\s+', ' ', tweet)
    # Normaliser les espaces
    tweet = re.sub(r'\s+', ' ', tweet)
    # Supprimer les chiffres
    tweet = re.sub(r'\d', '', tweet)
    # Convertir les emojis en texte
    tweet = emoji.demojize(tweet)
    # Tokenisation
    tokens = word_tokenize(tweet)
    # Supprimer les mots vides
    stop_words = set(stopwords.words('french'))
    filtered_tokens = [token for token in tokens if not token in stop_words]
    # Lemmatisation
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]
    # Reconstruire le tweet à partir des tokens lemmatisés
    clean_tweet = " ".join(lemmatized_tokens)
    return clean_tweet


def insert_tweet_sentiment(text, sentiment, subjectivite, date):
    # Se connecter à la base de données PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",  # Remplacez par votre nom d'utilisateur PostgreSQL
        password="root",  # Remplacez par votre mot de passe PostgreSQL
        database="twitter"  # Remplacez par le nom de votre base de données PostgreSQL
    )

    # Créer un curseur pour exécuter des requêtes SQL
    cursor = conn.cursor()

    # Créer la requête d'insertion
    sql = "INSERT INTO tweets_sentiment (tweet_text, sentiment, subjectivite, date_publication) VALUES (%s, %s, %s, %s)"
    values = (text, sentiment, subjectivite, date)

    # Exécuter la requête
    cursor.execute(sql, values)

    # Valider la transaction
    conn.commit()

    # Afficher un message de confirmation
    print(cursor.rowcount, "tweet inséré dans la table tweets_sentiment.")

    # Fermer la connexion à la base de données
    cursor.close()
    conn.close()

# Configuration des connexions au cluster Kafka
bootstrap_servers = ['localhost:9092']
topic_name = 'twitter_sentiment_final'

# Configuration du consommateur Kafka
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

# Lecture des messages du topic
for message in consumer:
    # Analyse de sentiment avec TextBlob
    print(message)
    text = message.value.decode('utf-8').split("|||")[0]
    date = message.value.decode('utf-8').split("|||")[1].split("T")[0]
    date_t = message.value.decode('utf-8').split("|||")[1].split("T")[1][:-1]
    date_p = date + " " + date_t
    cleaned_tweet = clean_tweet(text)
    sentiment = TextBlob(cleaned_tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()).sentiment[0]
    subjectivite = TextBlob(cleaned_tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer()).sentiment[1]
    insert_tweet_sentiment(text,sentiment,subjectivite,date_p)



