import pandas as pd
import json
import DatabaseAPI
import psycopg2

def map_to_boolean(value,match):
    return value == match

with open("mappings.json","r") as file:
    mappings = json.load(file)


df = pd.read_csv("csvs/OverallPlayerStats.csv")
player_columns = ["Player","Pos","Age","Tm","G","MP","PTS","TRB","AST","STL","BLK"]
player_types = {"Player":str,"Pos":str,"Age":int,"Tm":str,"G":int,"MP":float,"PTS":float,"TRB":float,"AST":float,"STL":float,"BLK":float}
player_df = df[player_columns]
player_df = player_df[~(player_df['Player'].str.contains('Player') & (player_df.index != 0))]
player_df.reset_index(drop=True, inplace=True)
for column, dtype in player_types.items():
    player_df[column] = player_df[column].astype(dtype)
player_df.rename(columns={'Tm': 'Team'}, inplace=True)
team_mapping = mappings["team_mapping"]
player_df['Team'] = player_df['Team'].replace(team_mapping)


df = pd.read_csv("csvs/Teams.csv",header=1)
team_columns = ["Team","G"]
team_df = df[team_columns][:-1]
team_types = {"Team":str,"G":int}
for column, dtype in team_types.items():
    team_df[column] = team_df[column].astype(dtype)


df = pd.read_csv("csvs/Schedule.csv")
df.rename(columns={'Start (ET)': 'Start','Visitor/Neutral':'Visitor','PTS':'Visitor_PTS','Home/Neutral':'Home','PTS.1':'Home_PTS',df.columns[6]:'Occured',df.columns[7]:'OT'}, inplace=True)
schedule_columns = ['Date', 'Start', 'Visitor', 'Visitor_PTS', 'Home', 'Home_PTS',
       'Occured', 'OT', 'Attend.', 'Arena']
schedule_df = df[schedule_columns]

schedule_df['OT'] = schedule_df['OT'].apply(lambda x: map_to_boolean(x, 'OT'))
schedule_df['Occured'] = schedule_df['Occured'].apply(lambda x: map_to_boolean(x, 'BoxScore'))
schedule_df = schedule_df[~(schedule_df['Date'].str.contains('Date') & (schedule_df.index != 0))]
schedule_df.fillna(0, inplace=True)
schedule_types = {'Date':str, 'Start':str, 'Visitor':str, 'Visitor_PTS':int, 'Home':str, 'Home_PTS':int,'Occured':bool, 'OT':bool, 'Attend.':str, 'Arena':str}

for column, dtype in schedule_types.items():
    schedule_df[column] = schedule_df[column].astype(dtype)


stadiums_df = schedule_df['Arena'].unique()
stadiums_df = pd.DataFrame(stadiums_df, columns=['Arena'])
#print(stadiums_df)

#print(schedule_df.dtypes)

def get_team_name(arena):
    for team, arena_name in mappings["arena_mapping"].items():
        if arena_name == arena:
            return team
    return None

stadiums_df["Team"] = stadiums_df['Arena'].apply(get_team_name)
stadiums_types = {"Arena": str, "Team": str}
for column, dtype in stadiums_types.items():
    stadiums_df[column] = stadiums_df[column].astype(dtype)

Western_conference_df = pd.read_csv("csvs/WesternConference.csv")
Eastern_conference_df = pd.read_csv("csvs/EasternConference.csv")

Western_conference_columns = ["Western Conference","W","L"]
Eastern_conference_columns = ["Eastern Conference","W","L"]                              

Western_conference_df = Western_conference_df[Western_conference_columns]
Eastern_conference_df= Eastern_conference_df[Eastern_conference_columns]
Western_conference_df.rename(columns={'Western Conference': 'Team'}, inplace=True)
Eastern_conference_df.rename(columns={'Eastern Conference': 'Team'}, inplace=True)
conference_types = { "Team": str, "W": int, "L": int }
for column, dtype in conference_types.items():
    Western_conference_df[column] = Western_conference_df[column].astype(dtype)
    Eastern_conference_df[column] = Eastern_conference_df[column].astype(dtype)
Western_conference_df['Team'] = Western_conference_df['Team'].str.split('(').str[0].str.strip()
Eastern_conference_df['Team'] = Eastern_conference_df['Team'].str.split('(').str[0].str.strip()

Injury_df = pd.read_csv('csvs/Injury.csv')
Injury_types = { 'Player':str, 'Team':str, 'Update': str, 'Description': str }
for column, dtype in Injury_types.items():
    Injury_df[column] = Injury_df[column].astype(dtype)

Coaches_df = pd.read_csv('csvs/Coaches.csv')
Coaches_df = Coaches_df.iloc[1:]
Coaches_df.columns = Coaches_df.iloc[0]

Coaches_columns = ["Coach","Tm","G","W","L"]
Coaches_df = Coaches_df[Coaches_columns]
Coaches_df = Coaches_df.iloc[1:]

Coaches_df.rename(columns={'Tm': 'Team'}, inplace=True)
Coaches_types = {"Coach":str, "Team":str, "G":int, "W":int, "L":int}
for column, dtype in Coaches_types.items():
    Coaches_df[column] = Coaches_df[column].astype(dtype)
team_mapping = mappings["team_mapping"]
Coaches_df['Team'] = Coaches_df['Team'].replace(team_mapping)

team_stats_df = pd.read_csv("csvs/TeamStats.csv")
Teamstats_columns = ['Team', 'MP', 'PTS','TRB', 'AST', 'STL', 'BLK']
team_stats_df = team_stats_df[Teamstats_columns]
Teamstats_types = {"Team":str, 'MP':float, 'PTS':float,'TRB':float, 'AST':float, 'STL':float, 'BLK':float}
for column, dtype in Teamstats_types.items():
    team_stats_df[column] = team_stats_df[column].astype(dtype)

dbname  = mappings["database_keys"]["dbname"]
user  = mappings["database_keys"]["user"]
password  = mappings["database_keys"]["password"]

conn = DatabaseAPI.connect_db(dbname,user,password)

#DatabaseAPI.create_database(dbname,user,password)

#DatabaseAPI.create_table(conn,'Teams',team_df,'Team_Id')
#DatabaseAPI.create_table(conn,'Players',player_df.loc[:,["Player","Pos","Age","Team","G","MP"]],'Player_id')
#DatabaseAPI.create_table(conn,'Player_Offensive_stats',player_df.loc[:,["Player","PTS","TRB","AST"]],'Player_id')
#DatabaseAPI.create_table(conn,'Player_Defensive_stats',player_df.loc[:,["Player","STL","BLK"]],'Player_id')
#DatabaseAPI.create_table(conn,'Stadiums',stadiums_df,'Stadium_id')
#DatabaseAPI.create_table(conn,'Western_conference',Western_conference_df,'Western_conference_id')
#DatabaseAPI.create_table(conn,'Eastern_conference',Eastern_conference_df,'Eastern_conference_id')
#DatabaseAPI.create_table(conn,'Injury',Injury_df,'Injury_Id')
#DatabaseAPI.create_table(conn,'Coaches',Coaches_df,'Coach_Id')
#DatabaseAPI.create_table(conn,'Team_Stats',team_stats_df,'Team_Id')


def update_and_rename_columns(table_name, source_column, target_column, foreign_key_column):
    try:
        

        cursor = conn.cursor()
        
        cursor.execute(
            
            f"ALTER TABLE public.\"{table_name}\" \
            ADD COLUMN {target_column} INTEGER"
        )
        
        cursor.execute(
            
            f"UPDATE public.\"{table_name}\" AS s \
            SET {target_column} = t.{target_column} \
            FROM public.\"Teams\" AS t \
            WHERE s.{source_column} = t.{source_column};"
        )
        cursor.execute(
            
            f"ALTER TABLE public.\"{table_name}\" \
            DROP COLUMN {source_column}"
        )

        cursor.execute(
            f"ALTER TABLE public.\"{table_name}\" \
            ADD CONSTRAINT fk_{table_name}_{foreign_key_column} FOREIGN KEY ({target_column}) REFERENCES public.\"Teams\" ({target_column});"
        )

        conn.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    


#update_and_rename_columns("Stadiums", "team", "team_id", "team_id")
#update_and_rename_columns("Injury", "team", "team_id", "team_id")
#update_and_rename_columns("Players", "team", "team_id", "team_id")
#update_and_rename_columns("Coaches", "team", "team_id", "team_id")
#update_and_rename_columns("Player_Offensive_stats", "team", "player_id", "player_id")
#update_and_rename_columns("Player_Defensive_stats", "team", "player_id", "player_id")
#update_and_rename_columns("Western_conference", "team", "team_id", "team_id")
#update_and_rename_columns("Eastern_conference", "team", "team_id", "team_id")
#update_and_rename_columns("Team_Stats", "team", "team_id", "team_id")