
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from selenium.common.exceptions import NoSuchElementException
import datetime
from datetime import timedelta
from kafka import KafkaProducer, KafkaConsumer
import psycopg2
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

# Spécifier le chemin du fichier exécutable de votre navigateur
driver = webdriver.Chrome()

# Configuration des connexions au cluster Kafka
bootstrap_servers = ['localhost:9092']
topic_name = 'twitter_sentiment_final'

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Ouvrir le site de Twitter
driver.get("https://twitter.com/i/flow/login")

# Attendre 5 secondes pour que la page se charge
time.sleep(5)

# Trouver l'élément "Nom d'utilisateur" et entrer votre nom d'utilisateur
username = driver.find_element("xpath","//*[@id=\"layers\"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div/div/div/div[5]/label")
username.send_keys("fredben870769")

# Cliquer sur le bouton "Suivant"
suivant = driver.find_element("xpath","//*[@id=\"layers\"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div/div/div/div[6]")
suivant.click()

time.sleep(2)
password = driver.find_element("xpath","//*[@id=\"layers\"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div[1]/div/div/div[3]/div/label/div/div[2]/div[1]/input")
password.send_keys("ADADadad123@")

Se_Connecter = driver.find_element("xpath","//*[@id=\"layers\"]/div/div/div/div/div/div/div[2]/div[2]/div/div/div[2]/div[2]/div[2]/div/div[1]/div/div/div")
Se_Connecter.click()
# Attendre 5 secondes pour que la page se charge
time.sleep(5)

# Effectuer la recherche "#ReformeDesRetraites"
search_box = driver.find_element("xpath",'//*[@id="react-root"]/div/div/div[2]/main/div/div/div/div[2]/div/div[2]/div/div/div/div[1]/div/div/div/form/div[1]/div/div/div/label/div[2]/div/input')
search_box.send_keys("#Reformedesretraites")
search_box.send_keys(Keys.RETURN)

# Attendre 5 secondes pour que la page se charge
time.sleep(5)

Latest = driver.find_element("xpath","//*[@id=\"react-root\"]/div/div/div[2]/main/div/div/div/div/div/div[1]/div[1]/div[2]/nav/div/div[2]/div/div[2]/a")
Latest.click()

# Attendre 5 secondes pour que la page se charge
time.sleep(5)


def insertion_bdd(tweet_tuple):
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="root",
        database="twitter"
    )

    # Création d'un curseur pour exécuter les requêtes SQL
    cursor = conn.cursor()

    # Construction de la requête SQL d'insertion
    insert_query = "INSERT INTO tweets (name, username, date_publication, tweet_text, tweet_reponse, tweet_like, tweet_retweet) VALUES (%s, %s, %s, %s, %s, %s, %s)"

    # Exécution de la requête SQL avec les valeurs du tuple
    cursor.execute(insert_query, tweet_tuple)

    # Validation de la transaction
    conn.commit()

    # Fermeture du curseur et de la connexion à la base de données
    cursor.close()
    conn.close()


def get_tweet_data(tweet):
    # récupération du name
    tweet_name = tweet.find_element("xpath", ".//span")
    name = tweet_name.text
    # récupération du username
    tweet_username = tweet.find_element("xpath", ".//span[contains(text(),'@')]")
    username = tweet_username.text
    # récupération date publication
    try:
        date_publication = tweet.find_element("xpath", ".//time").get_attribute('datetime')

    except NoSuchElementException:
        return
    # récuperation du tweet
    tweet_text = tweet.find_element("xpath", '//div[@data-testid="tweetText"]').text

    # récupération des réponse
    tweet_reponse = tweet.find_element("xpath", '//div[@data-testid="reply"]').text

    # récupération des retweet
    tweet_retweet = tweet.find_element("xpath", '//div[@data-testid="retweet"]').text

    # récupération des réponse
    tweet_like = tweet.find_element("xpath", '//div[@data-testid="like"]').text

    tweet_scrape = (name,username,date_publication,tweet_text,tweet_reponse,tweet_like,tweet_retweet)
    print(tweet_scrape)
    return tweet_scrape

#Décalaration de variable
tweets_data = []
tweet_ids = set()  # Utiliser un ensemble pour stocker les identifiants uniques
last_position = driver.execute_script("return window.pageYoffset;")  # Dernière position de scroll
scrolling = True

heure_dans_30s = (datetime.datetime.now() + timedelta(seconds=30)).time()
while scrolling:
    tweets = driver.find_elements("xpath", '//div[@data-testid="cellInnerDiv"]')
    for tweet in tweets:
        tweet_scraper = get_tweet_data(tweet)
        if tweet_scraper:
            tweet_id = ''.join(tweet_scraper)  # Vous pouvez utiliser une meilleure méthode pour créer un identifiant unique
            if tweet_id not in tweet_ids:
                tweet_ids.add(tweet_id)
                tweets_data.append(tweet_scraper)
                insertion_bdd(tweet_scraper)
                message = str(tweet_scraper[3]) + "|||" + str(tweet_scraper[2])
                print(tweet_scraper[3])
                producer.send(topic_name, message.encode('utf-8')) #envoi du tweet comme string
                    #producer.send("twitter_spark", tweet_scraper.encode('utf-8'))
    scroll_attempt = 0
    #Vérification de la barre de scroll
    driver.execute_script('window.scrollTo(0,document.body.scrollHeight);')
    time.sleep(1)
    position_actuelle = driver.execute_script("return window.pageYoffsert;")
    if last_position == position_actuelle:
        scroll_attempt += 1
        #fin du scroll
        if scroll_attempt >= 100:
            scrolling = False
            break
        else:
            time.sleep(2)
    else:
        last_position = position_actuelle

    # actualisation de la page toutes les 5 minutes
    heure_actuelle = datetime.datetime.now().time()
    if heure_dans_30s <= heure_actuelle:
        driver.refresh()
        print("page actualisé")
        heure_dans_30s = (datetime.datetime.now() + timedelta(seconds=30)).time()

# Fermer le navigateur
driver.quit()

