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

    find_player_sql = "SELECT player_id, player_current_rating FROM players WHERE player_name = :player_name"

    insert_players_sql = \
"""INSERT INTO players(
  player_name,
  player_current_rating,
  player_gender
) VALUES (
  :player_name,
  :player_rating,
  :player_gender
);"""

    update_player_rating_sql = \
"""UPDATE players
   SET player_current_rating = :player_rating
 WHERE player_id = :player_id
;"""

    insert_rosters_sql = \
"""INSERT INTO rosters(
  team_id,
  player_id
) VALUES (
  :team_id,
  :player_id
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
        tournament_table_list = soup.find("table", class_="tournamentList").find("tbody")
        tournament_trs = tournament_table_list.find_all("tr") 
        i = 0
        num_tournaments = len(tournament_trs)

        tournament_list = [] 
        #TODO need to load tournaments in chronological order to get the most recent player ratings
        for tournament_tr in tournament_trs:
            if i > 20: 
                break
            tournament_relative_url   = tournament_tr.attrs['data-url']
            tournament_url = f"{self.hostname}{tournament_relative_url}"
            tournament_title = tournament_tr.attrs['title']
        
            tournament_re = re.search(r"(.*)\s(.*)\s(.*)", tournament_title)
            tournament_format = tournament_re.group(1)
            tournament_level = tournament_re.group(2)
            tournament_date = tournament_re.group(3)
            (tournament_month, tournament_day, tournament_year) = tournament_date.split("/")
            tournament_date_formatted = f"{tournament_year.rjust(4, '0')}-{tournament_month.rjust(2, '0')}-{tournament_day.rjust(2, '0')}"

            tournament_list.append((tournament_date_formatted, tournament_format, tournament_level, tournament_url))            
            i = i + 1

        #print(f"Tournament list {tournament_list}") 
        tournament_list.sort(key=lambda y: y[0])
        #print(f"Tournament list sorted {tournament_list}") 

        j = 0 
        for tournament in tournament_list:
            (tournament_date_formatted, tournament_format, tournament_level, tournament_url) = tournament
            self.connection_obj = sqlite3.connect('yankee_mmr.db')
            try:  
                self.add_tournament_to_db(tournament_date_formatted, tournament_format, tournament_level, tournament_url)
                self.connection_obj.commit()
                print(f"Tournament {j} of {num_tournaments} {tournament_date_formatted} {tournament_format} {tournament_level} added")
            finally: 
                self.connection_obj.close()
            j = j + 1
            time.sleep(self.delay_between_page_requests)


    def add_tournament_to_db(self, tournament_date_formatted, tournament_format, tournament_level, tournament_url):
        page = requests.get(tournament_url)
        soup = BeautifulSoup(page.content, "html.parser")

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

                    
                    #(player_id, player_name, player_rating, player_gender, verify_membership, rerate) = player_info
                    #print(f"Player ID {player_id} Player Name {player_name} Rating {player_rating} Gender {player_gender}")
                    #print("x")
                    #print(player)

                    # Upsert the player record
                    params = {"player_name": player_name}
                    find_player_results = self.run_sql(self.find_player_sql, params).fetchone()
                    player_id = None
                    if find_player_results is None:
                        params = {"player_name": player_name, "player_rating": player_rating, "player_gender": player_gender}
                        self.run_sql(self.insert_players_sql, params)
                        player_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
                        #print(f"Inserted new player id {player_id}")
                    else:
                        player_id = find_player_results[0]
                        existing_rating = find_player_results[1]
                        if existing_rating != player_rating:
                            params = {"player_id": player_id, "player_rating": player_rating}
                            self.run_sql(self.update_player_rating_sql, params)
                            
                        #print(f"Found existing player id {player_id}")
                        
                    params = {"team_id": team_id, "player_id": player_id}
                    self.run_sql(self.insert_rosters_sql, params)
                    roster_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]

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
