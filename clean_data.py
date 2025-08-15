import pandas as pd

file_name_basics = r"D:\Baze\IMDB Data Pipeline\raw\name.basics.tsv"
file_title_basics = r"D:\Baze\IMDB Data Pipeline\raw\title.basics.tsv"
file_title_ratings = r"D:\Baze\IMDB Data Pipeline\raw\title.ratings.tsv"

name_basics = pd.read_csv(file_name_basics, sep='\t', low_memory=False, na_values='\\N')
title_basics = pd.read_csv(file_title_basics, sep='\t', low_memory=False, na_values='\\N')
title_ratings = pd.read_csv(file_title_ratings, sep='\t', low_memory=False, na_values='\\N')



def clean_name_basics(df):
    df = df[df['primaryProfession'].str.contains('director', na=False)].copy()
    df['birthYear'] = df['birthYear'].astype('Int64')
    df['deathYear'] = df['deathYear'].astype('Int64')
    df.dropna(subset=['primaryName'], inplace=True)
    df['primaryProfession'] = df['primaryProfession'].fillna("Unknown")
    df['knownForTitles'] = df['knownForTitles'].fillna("Unknown")
    df.reset_index(drop=True, inplace=True)
    return df


def clean_title_basics(df):
    df = df.copy()
    df['startYear'] = df['startYear'].astype('Int64')
    df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce').astype('Int64')
    df = df[(df['titleType'] == 'movie') & (df['startYear'] >= 2000) & (df['startYear'] <= 2024)]
    df.dropna(subset=['originalTitle', 'startYear', 'genres'], inplace=True)
    df.drop(columns='endYear', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def create_movie_directors(name_df, title_df):
    name_df = name_df.copy()
    name_df['knownForTitles'] = name_df['knownForTitles'].apply(lambda x: x.split(','))
    pairs_df = name_df.explode('knownForTitles')
    pairs_df['knownForTitles'] = pairs_df['knownForTitles'].str.strip()
    pairs_df = pairs_df[pairs_df['knownForTitles'].isin(title_df['tconst'])]
    movie_directors = pairs_df[['nconst', 'knownForTitles']].rename(
        columns={'nconst': 'DirectorID', 'knownForTitles': 'MovieID'}
    )
    return movie_directors.drop_duplicates()



def name_basics_to_import(df):
    df = df[['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession']].rename(
        columns={
            'nconst': 'DirectorID',
            'primaryName': 'Name_and_surname',
            'birthYear': 'Birth_Year',
            'deathYear': 'DeathYear',
            'primaryProfession': 'Primary_Profession'
        }
    )
    return df.drop_duplicates(subset='DirectorID')


def title_basics_to_import(df):
    df = df[['tconst', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres']].rename(
        columns={
            'tconst': 'MovieID',
            'primaryTitle': 'Title',
            'startYear': 'Year',
            'runtimeMinutes': 'RuntimeMinutes',
            'genres': 'Genres'
        }
    )
    return df.drop_duplicates(subset='MovieID')


def title_ratings_to_import(df):
    return df.rename(columns={
        'tconst': 'MovieID',
        'averageRating': 'Average_Rating',
        'numVotes': 'NumVotes'
    })


def movie_directors_to_import(name_df, title_df):
    return create_movie_directors(name_df, title_df)


if __name__ == "__main__":
    name_basics_clean = clean_name_basics(name_basics)
    title_basics_clean = clean_title_basics(title_basics)

    df_directors = name_basics_to_import(name_basics_clean)
    df_movies = title_basics_to_import(title_basics_clean)
    df_ratings = title_ratings_to_import(title_ratings)
    df_movie_directors = movie_directors_to_import(name_basics_clean, title_basics_clean)

    print(df_directors.head())
    print(df_movies.head())
    print(df_ratings.head())
    print(df_movie_directors.head())
