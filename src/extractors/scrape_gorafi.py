import requests
from bs4 import BeautifulSoup
import uuid
from datetime import datetime
import time
import json
from kafka import KafkaProducer

# --- Configuration Kafka ---
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Connecté au broker Kafka avec succès !")
except Exception as e:
    print(f"Erreur de connexion à Kafka : {e}")
    exit(1)

TOPIC_NAME = 'financial_news_stream'

def get_latest_article_links(limit=10):
    url = "https://www.legorafi.fr/"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    links = []
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if "/20" in href and "legorafi.fr" in href and href not in links:
            links.append(href)
            if len(links) >= limit:
                break
                
    return links

def scrape_gorafi_article(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    title_tag = soup.find('h1', class_='mvp-post-title')
    title = title_tag.text.strip() if title_tag else "Titre non trouvé"

    summary_tag = soup.find('span', class_='mvp-post-excerpt')
    summary = summary_tag.text.strip() if summary_tag else "Résumé non trouvé"

    date_tag = soup.find('meta', property='article:published_time')
    publish_date = date_tag['content'] if date_tag and date_tag.has_attr('content') else datetime.now().isoformat()

    return {
        "id_news": str(uuid.uuid4()),
        "title": title,
        "summary": summary,
        "event_date": publish_date, 
        "publish_date": publish_date,
        "source_name": "Le Gorafi"
    }

if __name__ == "__main__":
    print(f"Démarrage du collecteur Le Gorafi vers le topic Kafka '{TOPIC_NAME}'...")
    articles_deja_vus = set()
    
    try:
        while True:
            derniers_liens = get_latest_article_links(limit=10)
            
            for lien in derniers_liens:
                if lien not in articles_deja_vus:
                    data = scrape_gorafi_article(lien)
                    
                    if data:
                        producer.send(TOPIC_NAME, value=data)
                        print(f"Envoyé à Kafka : {data['title']}")
                        
                    articles_deja_vus.add(lien)
                    time.sleep(1) 
            
            producer.flush()
            
            print("En attente de nouveaux articles (pause de 1 heure)...")
            time.sleep(3600)
            
    except KeyboardInterrupt:
        print("\nArrêt manuel du collecteur.")
        producer.close()