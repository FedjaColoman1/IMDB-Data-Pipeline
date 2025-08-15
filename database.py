import sqlite3

DB_NAME = "movies.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def create_table(sql):
    """Helper funkcija za kreiranje tabele."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()

def create_table_Directors():
    create_table('''
        CREATE TABLE IF NOT EXISTS directors (
            DirectorID TEXT PRIMARY KEY,
            Name_and_surname TEXT,
            Birth_Year INT,
            DeathYear INT,
            Primary_Profession TEXT
        )
    ''')

def create_table_Movies():
    create_table('''
        CREATE TABLE IF NOT EXISTS movies (
            MovieID TEXT PRIMARY KEY,
            Title TEXT,
            Year INT,
            RuntimeMinutes INT,
            Genres TEXT
        )
    ''')

def create_table_Ratings():
    create_table('''
        CREATE TABLE IF NOT EXISTS ratings (
            MovieID TEXT,
            Average_Rating FLOAT,
            NumVotes INT,
            FOREIGN KEY (MovieID) REFERENCES movies(MovieID)
        )
    ''')

def create_table_Movies_Directors():
    create_table('''
        CREATE TABLE IF NOT EXISTS movies_directors (
            DirectorID TEXT,
            MovieID TEXT,
            FOREIGN KEY (DirectorID) REFERENCES directors(DirectorID),
            FOREIGN KEY (MovieID) REFERENCES movies(MovieID)
        )
    ''')
