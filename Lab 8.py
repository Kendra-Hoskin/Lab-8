import pandas as pd
import sqlite3

def connect_to_database(db_name):
    return sqlite3.connect(db_name)

def create_movies_table(con):
    create_bond_table_query = """
    CREATE TABLE IF NOT EXISTS movies (ID INTEGER NOT NULL UNIQUE,
                    Year INTEGER NOT NULL,
                    Movie TEXT NOT NULL UNIQUE,
                    Bond TEXT NOT NULL,
                    Bond_Car_MFG TEXT,
                    Depicted_Film_Loc TEXT,
                    Shooting_Loc TEXT,
                    BJB INTEGER,
                    Video_Game BOOLEAN,
                    Avg_User_IMDB REAL,
                    PRIMARY KEY("ID" AUTOINCREMENT)
                    )
    """
    cur = con.cursor()
    cur.execute(create_bond_table_query)
    con.commit()

def populate_movies_table(con, data_file):
    bond_df = pd.read_csv(data_file)
    cur = con.cursor()
    if cur.execute("SELECT COUNT(*) FROM movies").fetchone()[0] < 1:
        for row in bond_df.itertuples(index=False):
            cur.execute("""INSERT INTO movies (
                            Year,
                            Movie,
                            Bond,
                            Bond_Car_MFG,
                            Depicted_Film_Loc,
                            Shooting_Loc,
                            BJB,
                            Video_Game,
                            Avg_User_IMDB)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                           
                            (row.Year,
                            row.Movie,
                            row.Bond,
                            row.Bond_Car_MFG,
                            row.Depicted_Film_Loc,
                            row.Shooting_Loc,
                            row.BJB,
                            row.Video_Game,
                            row.Avg_User_IMDB))
    con.commit()

def add_movie(con, movie_data):
    cur = con.cursor()
    cur.execute("""INSERT INTO movies (
                    Year,
                    Movie,
                    Bond,
                    Bond_Car_MFG,
                    Depicted_Film_Loc,
                    Shooting_Loc,
                    BJB,
                    Video_Game,
                    Avg_User_IMDB)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", movie_data)
    con.commit()

def display_table_contents(con):
    print(pd.read_sql_query('SELECT * FROM movies', con))

def main():
    db_name = 'bond_movies.db'
    data_file = 'jamesbond.csv'
    no_time_to_die_data = (
        2021,
        'No Time to Die',
        'Daniel Craig',
        'Aston Martin',
        'Italy, Norway, Jamaica, UK, Faroe Islands',
        'Norway, Italy, UK, Jamaica, Faroe Islands',
        0,
        0,
        7.3
    )


    con = connect_to_database(db_name)

    try:
        create_movies_table(con)
        populate_movies_table(con, data_file)
        add_movie(con, no_time_to_die_data)

    finally:
        con.close()

if __name__ == "__main__":
    main()

