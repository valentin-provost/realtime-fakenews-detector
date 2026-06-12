import psycopg2

def init_db():
    """
    Se connecte à PostgreSQL et crée la table selon notre modèle de données.
    """
    print("Tentative de connexion à PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="market_news_db",
            user="admin",
            password="adminpassword",
            port="5432"
        )
        
        cursor = conn.cursor()
        
        # --- NOTRE MODELE DE DONNÉES EN SQL ---
        create_table_query = """
        CREATE TABLE IF NOT EXISTS financial_news (
            id_news UUID PRIMARY KEY,
            title TEXT UNIQUE,  -- LE BOUCLIER ANTI-DOUBLONS EST ICI !
            summary TEXT,
            event_date TIMESTAMP WITH TIME ZONE,
            publish_date TIMESTAMP WITH TIME ZONE,
            source_name VARCHAR(100),
            ml_trust_score FLOAT,  -- Sera rempli plus tard par le modèle ML
            is_verified BOOLEAN    -- Sera rempli plus tard (Vrai/Faux)
        );
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        
        print("Base de données initialisée : La table 'financial_news' est prête !")
        
    except Exception as e:
        print(f"Erreur lors de la création de la base : {e}")
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    init_db()