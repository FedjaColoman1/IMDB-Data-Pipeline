import pandas as pd
import sqlite3

from database import get_connection
from clean_data import (
    clean_name_basics,
    clean_title_basics,
    name_basics_to_import,
    title_basics_to_import,
    title_ratings_to_import,
    movie_directors_to_import
)
from database import (
    create_table_Directors,
    create_table_Movies,
    create_table_Movies_Directors,
    create_table_Ratings
)


def create_tables():
    create_table_Directors()
    create_table_Movies()
    create_table_Movies_Directors()
    create_table_Ratings()


def import_data():
    file_name_basics = r"D:\Baze\IMDB Data Pipeline\raw\name.basics.tsv"
    file_title_basics = r"D:\Baze\IMDB Data Pipeline\raw\title.basics.tsv"
    file_title_ratings = r"D:\Baze\IMDB Data Pipeline\raw\title.ratings.tsv"

    name_basics = pd.read_csv(file_name_basics, sep='\t', low_memory=False, na_values='\\N')
    title_basics = pd.read_csv(file_title_basics, sep='\t', low_memory=False, na_values='\\N')
    title_ratings = pd.read_csv(file_title_ratings, sep='\t', low_memory=False, na_values='\\N')

    name_basics_clean = clean_name_basics(name_basics)
    title_basics_clean = clean_title_basics(title_basics)

    conn = get_connection()

    name_basics_to_import(name_basics_clean).to_sql(
        name="directors",
        con=conn,
        if_exists="append",
        index=False
    )

    title_basics_to_import(title_basics_clean).to_sql(
        name="movies",
        con=conn,
        if_exists="append",
        index=False
    )

    title_ratings_to_import(title_ratings).to_sql(
        name="ratings",
        con=conn,
        if_exists="append",
        index=False
    )

    movie_directors_to_import(name_basics_clean, title_basics_clean).to_sql(
        name="movies_directors",
        con=conn,
        if_exists="append",
        index=False
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_tables()
    import_data()
