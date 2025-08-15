# IMDB Data Pipeline

This project processes and analyzes movie data from the IMDB datasets.  

## Project Structure
- `clean_data.py` – Cleans and prepares raw IMDB data  
- `database.py` – Creates SQLite database and tables  
- `import_data.py` – Imports cleaned data into the database  
- `analyze.py` – Performs analysis and creates charts  
- `raw/` – Folder for raw IMDB .tsv files (not included due to size)  

## Usage
1. Download IMDB datasets (`name.basics.tsv`, `title.basics.tsv`, `title.ratings.tsv`) from [IMDB Datasets](https://datasets.imdbws.com/) and place them in the `raw/` folder.  
2. Run scripts in order:
   ```bash
   python clean_data.py
   python database.py
   python import_data.py
   python analyze.py

Dependencies:

pandas

sqlite3

matplotlib

seaborn



Install dependencies with:
pip install pandas matplotlib seaborn


Note
This project is for educational purposes. Data files are not included due to size restrictions.
