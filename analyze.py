import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from database import get_connection

def run_query(sql):
    conn = get_connection()
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df

def query_top_10_movies():
    return run_query('''
        SELECT m.title, r.Average_Rating, r.NumVotes
        FROM movies m
        JOIN ratings r ON m.MovieID = r.MovieID
        WHERE r.NumVotes > 10000
        ORDER BY r.Average_Rating DESC
        LIMIT 10
    ''')

def query_most_active_directors():
    return run_query('''
        SELECT d.Name_and_surname AS Director,
               COUNT(m.MovieID) AS NumberOfMovies
        FROM directors d
        JOIN movies_directors md ON d.DirectorID = md.DirectorID
        JOIN movies m ON md.MovieID = m.MovieID
        GROUP BY d.Name_and_surname
        ORDER BY NumberOfMovies DESC
        LIMIT 10
    ''')

def query_average_yearly_ratings():
    return run_query('''
        SELECT m.Year,
               ROUND(AVG(r.Average_Rating), 2) AS 'Average Ratings'
        FROM movies m
        JOIN ratings r ON m.MovieID = r.MovieID
        GROUP BY m.Year
    ''')

def query_get_genres():
    movies_df = run_query('''
        SELECT Title, Genres
        FROM movies
    ''')
    movies_df['Genres'] = movies_df['Genres'].str.split(',')
    movies_exploded = movies_df.explode('Genres')
    movies_exploded['Genres'] = movies_exploded['Genres'].str.strip()
    genre_counts = movies_exploded['Genres'].value_counts().reset_index()
    genre_counts.columns = ['Genre', 'Count']
    return genre_counts

def chart_most_active_directors():
    plt.figure(figsize=(8, 6))
    sns.barplot(x="NumberOfMovies", y="Director", data=query_most_active_directors())
    plt.title("Top 10 most active directors")
    plt.xlabel("Number of movies")
    plt.ylabel("Director")
    plt.tight_layout()
    plt.show()

def chart_average_yearly_ratings():
    plt.figure(figsize=(10, 6))
    sns.lineplot(x='Year', y='Average Ratings', data=query_average_yearly_ratings(), marker="o")
    plt.title("Average yearly ratings")
    plt.xlabel("Year")
    plt.ylabel("Rating")
    plt.tight_layout()
    plt.show()

def chart_genres():
    plt.figure(figsize=(10, 6))
    sns.barplot(x='Genre', y='Count', data=query_get_genres())
    plt.title("Histogram of genres")
    plt.xlabel("Genre")
    plt.ylabel("Number of movies")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
