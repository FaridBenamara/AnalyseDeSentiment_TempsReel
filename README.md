# AnalyseDeSentiment_TempsReel
Projet Analyse de données et IA

## Objectif
Le but de ce projet est de collecter des tweets en rapport avec la réforme des retraites sur Twitter, les nettoyer et les stocker dans une base de données PostgreSQL. Les données seront également envoyées à un topic Kafka pour une analyse de sentiment avec textblob en temps réel et faire une visualisation avec Grafana.

## Configuration
| Prérequis       | Version    |
|----------------|------------|
| Python         | 3.x        |
| Dépendances Python | selenium, kafka-python, psycopg2, nltk, langdetect |
| WebDriver      | Par exemple, ChromeDriver pour Selenium |

## WebDriver Selenium Chrome
- Téléchargez le WebDriver pour Selenium à partir de [ce lien](https://chromedriver.chromium.org/downloads)

## Installation des dépendances
```bash
pip install -r requirements.txt
```
## Config Kafka (Dossier Kafka)
```bash
Windows :  C:\kafka\bin\zookeeper-server-start.bat
Linux : nohup $KAFKA_HOME/bin/zookeeper-server-start.sh
```
