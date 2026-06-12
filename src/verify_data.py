import pandas as pd
import psycopg2

# Connexion à la base
conn = psycopg2.connect(
    host="localhost", database="market_news_db", user="admin", password="adminpassword"
)

# Récupération des données dans un DataFrame
df = pd.read_sql_query("SELECT * FROM financial_news;", conn)

# Affichage des données
print(f"Nombre total d'articles : {len(df)}")
print("\nLes 5 derniers articles :")
print(df[['title','summary']].head())

conn.close()