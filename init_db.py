import requests
from bs4 import BeautifulSoup
import csv
import re
import sqlite3

class InitDB():

    def run(self):
        self.create_db()

    def run_sql(self, sql):
        print(f"Running sql: {sql}")
        connection_obj = sqlite3.connect('yankee_mmr.db')
        cursor_obj = connection_obj.cursor()
        cursor_obj.execute(sql)
        connection_obj.close() 

    def create_db(self):
        ##### Tournaments Table #####
        drop_tournament_table = """DROP TABLE IF EXISTS tournaments;"""
        self.run_sql(drop_tournament_table)
        create_tournament_table = \
"""CREATE TABLE tournaments (
  tournament_id   INTEGER PRIMARY KEY NOT NULL, 
  format          VARCHAR(10) NOT NULL,
  level           VARCHAR(10) NOT NULL,
  tournament_date CHAR(10)    NOT NULL
 ); """

        self.run_sql(create_tournament_table)

        ##### Teams Table #####
        drop_teams_table = """DROP TABLE IF EXISTS teams;"""
        self.run_sql(drop_teams_table)
        create_teams_table = \
"""CREATE TABLE teams (
  team_id INTEGER PRIMARY KEY NOT NULL, 
  tournament_id INTEGER NOT NULL, 
  team_name VARCHAR(255) NOT NULL, 
  team_rating VARCHAR(10), 
  FOREIGN KEY (tournament_id) REFERENCES tournaments(tournament_id)
);"""
        self.run_sql(create_teams_table)

        ##### Rosters Table #####
        drop_players_table = """DROP TABLE IF EXISTS players;"""
        self.run_sql(drop_players_table)
        create_players_table = \
"""CREATE TABLE players (
  player_id INTEGER PRIMARY KEY NOT NULL, 
  team_id INTEGER NOT NULL, 
  player_name VARCHAR(255) NOT NULL, 
  player_rating VARCHAR(10), 
  player_gender VARCHAR(10), 
  verify_membership VARCHAR(10), 
  rerate VARCHAR(10), 
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);"""
        self.run_sql(create_players_table)

        ##### Match Results Table #####
        drop_match_results_table = """DROP TABLE IF EXISTS match_results;"""
        self.run_sql(drop_match_results_table)
        create_match_results_table = \
"""CREATE TABLE match_results (
  winning_team_id INTEGER NOT NULL, 
  losing_team_id INTEGER NOT NULL,
  match_description VARCHAR(255),
  FOREIGN KEY (winning_team_id) REFERENCES teams(team_id),
  FOREIGN KEY (losing_team_id)  REFERENCES teams(team_id)
);"""
        self.run_sql(create_match_results_table)

        ##### Pool Play Results Table #####
        drop_pool_results_table = """DROP TABLE IF EXISTS pool_results;"""
        self.run_sql(drop_pool_results_table)
        create_pool_results_table = \
"""CREATE TABLE pool_results (
  team_id INTEGER NOT NULL, 
  number_wins INTEGER NOT NULL,
  number_losses INTEGER NOT NULL,
  FOREIGN KEY (team_id)  REFERENCES teams(team_id)
);"""
        self.run_sql(create_pool_results_table)

if __name__ == '__main__':
    InitDB().run()
