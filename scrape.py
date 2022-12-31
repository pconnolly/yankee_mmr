import requests
from bs4 import BeautifulSoup
import csv
import re
import sqlite3

class Scrape():

    insert_tournament_sql = \
"""INSERT INTO tournaments(
  tournament_format,
  tournament_level,
  tournament_date
) VALUES (
'{tournament_format}',
'{tournament_level}',
'{tournament_date}'
);"""

    insert_teams_sql = \
"""INSERT INTO teams(
  tournament_id,
  team_name
) VALUES (
'{tournament_id}',
'{team_name}'
);"""

    insert_players_sql = \
"""INSERT INTO players(
  team_id,
  player_name,
  player_rating,
  player_gender,
  verify_membership,
  rerate
) VALUES (
'{team_id}',
'{player_name}',
'{player_rating}',
'{player_gender}',
'{verify_membership}',
'{rerate}'
);"""

    insert_match_results_sql = \
"""INSERT INTO match_results(
  tournament_id,
  winning_team_name,
  losing_team_name,
  match_description
) VALUES ({tournament_id},
       '{winning_team_name}',
       '{losing_team_name}',
       '{match_description}'
);"""

    insert_pool_results_sql = \
"""INSERT INTO pool_results(
  tournament_id,
  team_name,
  pool_name,
  number_wins,
  number_losses
) VALUES (
  {tournament_id},
  '{pool_name}',
  '{team_name}',
  {number_wins},
  {number_losses}
);"""

    def run(self):
        self.hostname = "http://yankee.org"

        tournament_results_url = f"{self.hostname}/tournaments/results"
        tournaments = self.add_tournaments_to_db(tournament_results_url)

    def run_sql(self, sql):
        print(f"Running sql {sql}")
        cursor_obj = self.connection_obj.cursor()
        return cursor_obj.execute(sql)

    def add_tournaments_to_db(self, tournament_results_url):
        page = requests.get(tournament_results_url)
        soup = BeautifulSoup(page.content, "html.parser")
        tournament_list = soup.find("table", class_="tournamentList").find("tbody")
        tournament_trs = tournament_list.find_all("tr") 
        i = 0
        for tournament_tr in tournament_trs:
            if i > 4: 
                exit()
            tournament_relative_url   = tournament_tr.attrs['data-url']
            tournament_url = f"{self.hostname}{tournament_relative_url}"
            tournament_title = tournament_tr.attrs['title']
            print(f"Tournament {tournament_title} at url {tournament_url}")
            self.connection_obj = sqlite3.connect('yankee_mmr.db')
            try:  
                self.add_tournament_to_db(tournament_url)
                self.connection_obj.commit()
            finally: 
                self.connection_obj.close()
            i = i + 1


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
        self.run_sql(self.insert_tournament_sql.format(tournament_format=tournament_format, tournament_level=tournament_level, tournament_date=tournament_date_formatted))
        tournament_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
        print(f"Inserted tournament id {tournament_id}")

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
            print(f"Team: {team_name_clean}")
            self.run_sql(self.insert_teams_sql.format(tournament_id=tournament_id, team_name=team_name_clean))
            team_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
            print(f"Inserted team id {team_id}")

            players = team.find_all("tr")
            for player in players:
                #print(f"Player {player}")
                player_info = player.find_all("td")

                # Eat the table header
                if(len(player_info) > 0):
                    #print(f"Player Info {player_info}")
                    player_id = player_info[0].text.strip()
                    player_name = player_info[1].text.strip()
                    player_rating = player_info[2].text.strip()
                    player_gender = player_info[3].text.strip()
                    verify_membership = player_info[4].text.strip()
                    rerate = player_info[5].text.strip()
                    #(player_id, player_name, player_rating, player_gender, verify_membership, rerate) = player_info
                    print(f"Player ID {player_id} Player Name {player_name} Rating {player_rating} Gender {player_gender}")
                    #print("x")
                    #print(player)
                    self.run_sql(self.insert_players_sql.format(team_id=team_id, player_name=player_name, player_rating=player_rating, player_gender=player_gender, verify_membership=verify_membership, rerate=rerate))
                    player_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
                    print(f"Inserted player id {player_id}")

        results_url = None
        # Find the results page, if it exists 
        for a in soup.find_all('a', href=True):
            if a.text.strip() == 'READ THE TOURNAMENT RECAP':
                results_url_relative = a['href']
                results_url = f"{self.hostname}{results_url_relative}"
                print(f"Found it!: {results_url}")
        if results_url != None:
            (individual_match_results, pool_results) = self.get_results(results_url)
            for match_description, (winning_team_name, losing_team_name) in individual_match_results.items():
                print(f"Match desc {match_description} winning team {winning_team_name} losing {losing_team_name}") 
                self.run_sql(self.insert_match_results_sql.format(tournament_id=tournament_id, winning_team_name=winning_team_name, losing_team_name=losing_team_name, match_description=match_description))
                match_results_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
                print(f"Inserted match_results_id {match_results_id}")
                #TODO Verify the rowcount is 1 or the insert didn't work

            for (pool_name, team_results_dict) in pool_results.items():
                for (team_name, team_record) in team_results_dict.items():
                    number_wins = team_record.split("-")[0]
                    number_losses = team_record.split("-")[1]
                    self.run_sql(self.insert_pool_results_sql.format(tournament_id=tournament_id, pool_name=pool_name, team_name=team_name, number_wins=number_wins, number_losses=number_losses))
                    pool_results_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
                    print(f"Inserted pool_results_id {match_results_id}")

    def get_results(self, results_url):
        page = requests.get(results_url)
        soup = BeautifulSoup(page.content, "html.parser")

        results_summary_div = soup.find("div", class_="summary") 
        p_tags = results_summary_div.find_all("p")
        pool_dict = {} 
        individual_match_results = {}
        current_index = 0
        for p_tag in p_tags:
            if "defeated" in p_tag.text:
                # Should use a regex here
                colon_index = p_tag.text.index(":")
                defeated_index = p_tag.text.index("defeated")
                winning_team = p_tag.text[colon_index + 1:defeated_index].strip()
                losing_team = p_tag.text[defeated_index + len("defeated"):].strip()
                label = p_tag.text[:colon_index]
                individual_match_results[label] = (winning_team, losing_team)
                print(f"Winning Team: {winning_team} Losing Team {losing_team}")

            if "Pool " in p_tag.text:
                pool_dict[p_tag.text.strip(":")] = current_index
            print(f"P tag {p_tag}")
            current_index = current_index + 1
        print(f"Pool indices {pool_dict}")
        num_pools = len(pool_dict)
        previous_item = None
        pool_dict_with_teams = {}
        for pool_item in pool_dict.items():
            (key, value) = pool_item
            if previous_item is not None:
                (prev_key, prev_value) = previous_item
                print(f"previous_item: {previous_item}")
                print(f"previous_item_key: {key}")
                print(f"previous_item_value: {value}")
                start_index = prev_value + 1
                end_index = value
                pool_name = prev_key
                print(f"{pool_name} start Index: {start_index} End Index: {end_index}")

                pool_dict_with_teams[pool_name] = p_tags[start_index:end_index]
            previous_item = pool_item

        (prev_key, prev_value) = previous_item
        start_index = prev_value + 1
        end_index = len(p_tags)
        pool_name = prev_key
        pool_dict_with_teams[pool_name] = p_tags[start_index:end_index]
        print(f"{pool_name} start Index: {start_index} End Index: {end_index}")
        print(pool_dict_with_teams)
       
        pool_results = {}
        for pool in pool_dict_with_teams.items():
            pool_records = {}
            (pool_name, team_p_tags) = pool
            for team_p_tag in team_p_tags:
                team_text = team_p_tag.text 
                first_space_index = team_text.find(" ") 
                team_record = team_text[:first_space_index].strip()
                team_name = team_text[first_space_index:].strip()
                print(f"Team {team_name} had record {team_record}")
                pool_records[team_name] = team_record
            pool_results[pool_name] = pool_records

        print(f"Individual match results {individual_match_results}")
        print(f"Pools {pool_results}")
        return (individual_match_results, pool_results)

if __name__ == '__main__':
    Scrape().run()
