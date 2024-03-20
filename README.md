# AnalyseDeSentiment_TempsReel
Projet Analyse de données et IA 


## Objectif
Le but de ce projet est de collecter des tweets en rapport avec la réforme des retraites sur Twitter, les nettoyer et les stocker dans une base de données PostgreSQL. Les données seront également envoyées à un topic Kafka pour une analyse de sentiment avec textblob en temps réel et faire une visualisation avec grafana.

## Configuration
- Python 3.x
- Dépendances Python : `selenium`, `kafka-python`, `psycopg2`, `nltk`, `langdetect`
- WebDriver (par exemple, ChromeDriver) pour Selenium

## Installation des dépendances
```bash
pip install -r requirements.txt
```
## Config Kafka (Dossier Kafka)
