import json
import psycopg2
from kafka import KafkaConsumer
from transformers import pipeline

print("Chargement du modèle IA en cours...")
# On télécharge directement le modèle depuis le hub Hugging Face
fake_news_detector = pipeline("text-classification", model="JosuMSC/fake-news-detector")
print("Modèle IA chargé et prêt à détecter les fakes news !")

# --- 1. Configuration Kafka ---
TOPIC_NAME = 'financial_news_stream'

try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Connecté à Kafka : En écoute des nouveaux articles...")
except Exception as e:
    print(f"Erreur Kafka : {e}")
    exit(1)

# --- 2. Configuration PostgreSQL ---
try:
    conn = psycopg2.connect(
        host="localhost", database="market_news_db", user="admin", password="adminpassword", port="5432"
    )
    print("Connecté à PostgreSQL.")
except Exception as e:
    print(f"Erreur PostgreSQL : {e}")
    exit(1)

# --- 3. Boucle de Traitement ---
print("\nAttente de messages...\n")

try:
    for message in consumer:
        article = message.value
        title = article['title']
        
        # --- ÉTAPE ML : L'IA LIT L'ARTICLE ---
        # On donne le titre (et le résumé si dispo) à lire à l'IA
        texte_a_analyser = f"{title}. {article['summary']}"
        
        # L'IA renvoie un résultat type : {'label': 'FAKE', 'score': 0.98}
        prediction = fake_news_detector(texte_a_analyser[:512])[0] # On limite à 512 mots pour BERT
        
        # On transforme le résultat en un "Trust Score" (Score de confiance) entre 0 et 1
        # Si l'IA dit que c'est VRAI, le score est direct. Si c'est FAUX, on inverse la probabilité.
        label = str(prediction['label']).upper()
        if "REAL" in label or "TRUE" in label or "1" in label:
            ml_trust_score = round(prediction['score'], 4)
        else:
            ml_trust_score = round(1.0 - prediction['score'], 4)
            
        print(f"IA Score : {ml_trust_score:.2f} | Article : {title[:50]}...")
        
        # --- ÉTAPE BDD : INSERTION AVEC LE SCORE ---
        insert_query = """
        INSERT INTO financial_news 
        (id_news, title, summary, event_date, publish_date, source_name, ml_trust_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (title) DO NOTHING;
        """
        
        valeurs = (
            article['id_news'], article['title'], article['summary'],
            article['event_date'], article['publish_date'], article['source_name'],
            ml_trust_score # <-- NOTRE NOUVEAU SCORE EST AJOUTÉ ICI
        )
        
        cursor = conn.cursor()
        cursor.execute(insert_query, valeurs)
        conn.commit()
        
        if cursor.rowcount == 1:
            print("Sauvegardé en BDD")
        else:
            print("Doublon ignoré")
            
        cursor.close()

except KeyboardInterrupt:
    print("\nArrêt manuel du consommateur.")
finally:
    if 'conn' in locals(): conn.close()
    if 'consumer' in locals(): consumer.close()