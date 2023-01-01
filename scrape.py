import requests
from bs4 import BeautifulSoup
import csv
import re
import sqlite3
import time

class Scrape():

    # Wait x seconds between page requests so we don't take down the yankee website
    delay_between_page_requests = 1

    insert_tournament_sql = \
"""INSERT INTO tournaments(
  tournament_format,
  tournament_level,
  tournament_date
) VALUES (
  :tournament_format,
  :tournament_level,
  :tournament_date
);"""

    insert_teams_sql = \
"""INSERT INTO teams(
  tournament_id,
  team_name
) VALUES (
  :tournament_id,
  :team_name
);"""

    insert_players_sql = \
"""INSERT INTO players(
  team_id,
  player_name,
  player_rating,
  player_gender
) VALUES (
  :team_id,
  :player_name,
  :player_rating,
  :player_gender
);"""

    insert_result_text_sql = \
"""INSERT INTO result_text(
  tournament_id,
  row_nbr,
  result_text
) VALUES (
  :tournament_id,
  :row_nbr,
  :result_text
);"""

    def run(self):
        self.hostname = "http://yankee.org"

        tournament_results_url = f"{self.hostname}/tournaments/results"
        tournaments = self.add_tournaments_to_db(tournament_results_url)

    def run_sql(self, sql, params = {}):
        #print(f"Running sql {sql}")
        cursor_obj = self.connection_obj.cursor()
        return cursor_obj.execute(sql, params)

    def add_tournaments_to_db(self, tournament_results_url):
        page = requests.get(tournament_results_url)
        soup = BeautifulSoup(page.content, "html.parser")
        tournament_list = soup.find("table", class_="tournamentList").find("tbody")
        tournament_trs = tournament_list.find_all("tr") 
        i = 0
        num_tournaments = len(tournament_trs)
        for tournament_tr in tournament_trs:
            if i > 20: 
                exit()
            tournament_relative_url   = tournament_tr.attrs['data-url']
            tournament_url = f"{self.hostname}{tournament_relative_url}"
            tournament_title = tournament_tr.attrs['title']
            self.connection_obj = sqlite3.connect('yankee_mmr.db')
            try:  
                self.add_tournament_to_db(tournament_url)
                self.connection_obj.commit()
                print(f"Tournament {i} of {num_tournaments} {tournament_title} added")
            finally: 
                self.connection_obj.close()
            i = i + 1
            time.sleep(self.delay_between_page_requests)


    def add_tournament_to_db(self, tournament_url):
        page = requests.get(tournament_url)
        soup = BeautifulSoup(page.content, "html.parser")

        tournament_div = soup.find("div", class_="tourneyName") 
        tournament_string = tournament_div.find("h1").find("span").text.strip()
        tournament_re = re.search(r"(.*)\s(.*)\s(.*)", tournament_string)
        tournament_format = tournament_re.group(1)
        tournament_level = tournament_re.group(2)
        tournament_date = tournament_re.group(3)
        (tournament_month, tournament_day, tournament_year) = tournament_date.split("/")
        tournament_date_formatted = f"{tournament_year}-{tournament_month}-{tournament_day}"
        #print(f"Tournament format: {tournament_format} level: {tournament_level} date: {tournament_date_formatted}")
        params = {"tournament_format": tournament_format, "tournament_level": tournament_level, "tournament_date": tournament_date_formatted}
        self.run_sql(self.insert_tournament_sql, params)
        tournament_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
        #print(f"Inserted tournament id {tournament_id}")

        confirmed_teams = soup.find("span", string="CONFIRMED TEAMS").parent.parent.parent
        #print(f"confirmed_teams {confirmed_teams}")
        teams = confirmed_teams.find_all("table", class_="teamRosterTable") 
        for team in teams:
            team_div = team.parent.find("div", class_="teamName")
            team_name = team_div.find("div", class_="name").text
            #print(f"team_name_raw: {team_name}")
            date_index = team_name.find("-")
            #print(f"date_index: {date_index}")
            team_name_clean = team_name[:date_index].strip()
            #print(f"Team: {team_name_clean}")
            params = {"tournament_id":tournament_id, "team_name": team_name_clean}
            self.run_sql(self.insert_teams_sql, params)
            team_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
            #print(f"Inserted team id {team_id}")

            players = team.find_all("tr")
            for player in players:
                #print(f"Player {player}")
                player_info = player.find_all("td")

                # Eat the table header
                if(len(player_info) > 0):
                    #print(f"Player Info {player_info}")
                    player_id = player_info[0].text.strip()
                    # An asterisk on a name indicates a captain, remove it
                    player_name = player_info[1].text.strip().strip("*")
                    player_rating = player_info[2].text.strip()
                    player_gender = player_info[3].text.strip()

                    # Verify membership and rerate only reflect the current value, so they aren't valuable to store
                    # For example if a player was re-rateable on Jan 1 tournament they will show as a "no" here 
                    verify_membership = player_info[4].text.strip()
                    rerate = player_info[5].text.strip()
                    #(player_id, player_name, player_rating, player_gender, verify_membership, rerate) = player_info
                    #print(f"Player ID {player_id} Player Name {player_name} Rating {player_rating} Gender {player_gender}")
                    #print("x")
                    #print(player)
                    params = {"team_id": team_id, "player_name": player_name, "player_rating": player_rating, "player_gender": player_gender}
                    self.run_sql(self.insert_players_sql, params)
                    player_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
                    #print(f"Inserted player id {player_id}")

        results_url = None
        # Find the results page, if it exists 
        for a in soup.find_all('a', href=True):
            if a.text.strip() == 'READ THE TOURNAMENT RECAP':
                results_url_relative = a['href']
                results_url = f"{self.hostname}{results_url_relative}"
                #print(f"Found it!: {results_url}")
        if results_url != None:
            self.write_result_text(tournament_id, results_url)

    def write_result_text(self, tournament_id, results_url):
        page = requests.get(results_url)
        soup = BeautifulSoup(page.content, "html.parser")

        results_summary_div = soup.find("div", class_="summary") 
        p_tags = results_summary_div.find_all("p")
        pool_dict = {} 
        individual_match_results = {}
        current_index = 0
        for p_tag in p_tags:
            params = {"tournament_id": tournament_id, "row_nbr": current_index, "result_text": p_tag.text}
            self.run_sql(self.insert_result_text_sql, params)
            result_text_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
            current_index = current_index + 1

if __name__ == '__main__':
    Scrape().run()
