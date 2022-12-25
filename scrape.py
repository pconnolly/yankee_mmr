import requests
from bs4 import BeautifulSoup

class Scrape():
    def run(self):
        hostname = "http://yankee.org"
        tournament_url = f"{hostname}/tournaments/cr-c-12-22-2019"
        page = requests.get(tournament_url)
        soup = BeautifulSoup(page.content, "html.parser")

        tournament_div = soup.find("div", class_="tourneyName") 
        tournament = tournament_div.find("h1").find("span").text.strip()
        print(f"Tournament: {tournament}")


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

        results_url = None
        # Find the results page, if it exists 
        for a in soup.find_all('a', href=True):
            if a.text.strip() == 'READ THE TOURNAMENT RECAP':
                results_url_relative = a['href']
                results_url = f"{hostname}{results_url_relative}"
                print(f"Found it!: {results_url}")
        if results_url != None:
            self.get_results(results_url)

    def get_results(self, results_url):
        page = requests.get(results_url)
        soup = BeautifulSoup(page.content, "html.parser")

        results_summary_div = soup.find("div", class_="summary") 
        p_tags = results_summary_div.find_all("p")
        pool_indexes = {} 
        current_index = 0
        for p_tag in p_tags:
            if "defeated" in p_tag.text:
                # Should use a regex here
                colon_index = p_tag.text.index(":")
                defeated_index = p_tag.text.index("defeated")
                winning_team = p_tag.text[colon_index:defeated_index].strip()
                losing_team = p_tag.text[defeated_index + len("defeated"):].strip()
                print(f"Winning Team: {winning_team} Losing Team {losing_team}")

            if "Pool " in p_tag.text:
                pool_indexes[p_tag.text] = current_index
            print(f"P tag {p_tag}")
            current_index = current_index + 1
        print(f"Pool indices {pool_indexes}")

if __name__ == '__main__':
    Scrape().run()
