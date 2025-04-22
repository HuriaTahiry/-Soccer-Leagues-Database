import pymysql
import configparser
import csv


config = configparser.ConfigParser()
config.read('credentials.txt')
dbhost = config['csc']['dbhost']
dbuser = config['csc']['dbuser']
dbpw = config['csc']['dbpw']

# Choose schema
dbschema = 'htahiry'


dbconn = pymysql.connect(
    host=dbhost,
    user=dbuser,
    passwd=dbpw,
    db=dbschema,
    use_unicode=True,
    charset='utf8mb4',
    autocommit=True
)
cursor = dbconn.cursor()


def populate_tables(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute(
                "INSERT IGNORE INTO Project1_LeagueName (LeagueName) VALUES (%s)", (row['League'],)
            )
            cursor.execute("SELECT League_ID FROM Project1_LeagueName WHERE LeagueName = %s", (row['League'],))
            League_ID = cursor.fetchone()
            #here is the problmm
            cursor.execute(
                "INSERT IGNORE INTO Project1_TeamNames (TeamName, League_ID) VALUES (%s, %s)",
                (row['team'], League_ID)
            )
            cursor.execute("SELECT Team_ID FROM Project1_TeamNames WHERE TeamName = %s", (row['team'],))
            Team_ID = cursor.fetchone()


            cursor.execute(
                """
                INSERT INTO Project1_teamData (Team_ID, Year, NumWins, NumDraws, NumLoses, NumScored, NumMissed, Pts, NumMatches)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (Team_ID, row['Year'], row['wins'], row['draws'], row['loses'], row['scored'], row['missed'], row['pts'], row['matches'])
            )


if __name__ == "__main__":
    try:

        csv_file = '/Users/huriatahiry/PycharmProjects/DatabaseProject1/Footballdata.csv'
        populate_tables(csv_file)
        print("Data r in")
    except Exception as e:
        print(f"error {e}")
    finally:
        cursor.close()
        dbconn.close()
