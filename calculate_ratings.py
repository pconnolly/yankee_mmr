import re
import sqlite3
import math
import glicko2

class CalculateRatings():

    default_mmr = 1500

    default_rd = 350

    default_num_players = 6 

    q = (math.log(10) / 400)

    get_tournaments_sql = "SELECT tournament_id, tournament_format, tournament_level, tournament_date FROM tournaments WHERE tournament_id = 2 ORDER BY tournament_date, tournament_format, tournament_level, tournament_id"

    get_tournament_pools_sql = "SELECT DISTINCT pool_name FROM pool_results WHERE tournament_id = :tournament_id"

    get_tournament_pool_results_sql = "SELECT team_id, number_wins, number_losses FROM pool_results WHERE tournament_id = :tournament_id AND pool_name = :pool_name"

    get_roster_players_sql = "SELECT player_id FROM rosters WHERE team_id = :team_id"

    get_player_current_rating_sql = "WITH all_player_ratings AS (SELECT pm.mmr, pm.rd, ROW_NUMBER() OVER (ORDER BY tournament_id DESC) AS rnk FROM player_mmr_history pm WHERE player_id = :player_id) SELECT mmr, rd FROM all_player_ratings WHERE rnk = 1"

    def run_sql(self, sql, params = {}):
        #print(f"Running sql {sql}")
        cursor_obj = self.connection_obj.cursor()
        return cursor_obj.execute(sql, params)

    def run(self):
        print(f"q is {self.q}")
        self.connection_obj = sqlite3.connect('yankee_mmr.db')
        try:
            tournaments_to_rate = self.run_sql(self.get_tournaments_sql).fetchall()
            num_tournaments = len(tournaments_to_rate)
            i = 0 
            for tournament in tournaments_to_rate:
                (tournament_id, tournament_format, tournament_level, tournament_date) = tournament
                print(f"Calculating MMR for {tournament_id}: {tournament_format} {tournament_level} {tournament_date}")
                self.calculate_pool_mmr(tournament)
                print(f"Completed {i+1} of {num_tournaments} ({((i+1) / num_tournaments) * 100}%)")
                i = i + 1
            self.connection_obj.commit()
        finally:
            self.connection_obj.close()

    def calculate_pool_mmr(self, tournament):
        (tournament_id, tournament_format, tournament_level, tournament_date) = tournament
        params = {"tournament_id": tournament_id}
        pools = self.run_sql(self.get_tournament_pools_sql, params).fetchall()
        for pool in pools:
            pool_name = pool[0]
            print(f"Rating pool {pool_name} in tournament {tournament_id}")
            params = {"tournament_id": tournament_id, "pool_name": pool_name}
            pool_results = self.run_sql(self.get_tournament_pool_results_sql, params).fetchall()
            pool_ratings = {}
            number_of_expected_teams = None
            # Go through the pool once to calculate average mmr and rd
            for pool_result in pool_results:
                (team_id, number_wins, number_losses) = pool_result
                number_of_expected_opponents = (number_wins + number_losses) / 2
                print(f"Pool result team id {team_id} with {number_wins} wins and {number_losses} losses")
                params = {"team_id": team_id}
                team_players = self.run_sql(self.get_roster_players_sql, params).fetchall()
                sum_of_team_mmr = 0
                sum_of_team_rd = 0
                number_of_players = 0
                for player in team_players:
                    player_id = player[0]
                    #print(f"Getting ratings for player {player_id}")
                    params = {"player_id": player_id}
                    player_rating_result = self.run_sql(self.get_player_current_rating_sql, params).fetchone()
                    if player_rating_result is not None:
                        (player_mmr, player_rd) = player_rating_result
                    else: 
                        #print(f"Using default ratings for player {player_id}")
                        player_mmr = self.default_mmr
                        player_rd = self.default_rd

                    sum_of_team_mmr = sum_of_team_mmr + player_mmr
                    sum_of_team_rd = sum_of_team_rd + player_rd
                    number_of_players = number_of_players + 1
                    print(f"Adding {player_id} to team {team_id} for a total mmr of {sum_of_team_mmr} and rd {sum_of_team_rd}")

                pool_ratings[team_id] = (sum_of_team_mmr, sum_of_team_rd, number_of_players)

            # Add missing teams with default ratings
            number_found_opponents = len(pool_results) - 1
            missing_teams = int(number_of_expected_opponents - number_found_opponents)
            if missing_teams > 0:
                print(f"Missing {missing_teams} teams from tournament {tournament_id}. Adding default players to {tournament_format} {tournament_level} {tournament_date} for pool {pool_name}")
            for i in range(missing_teams):
                pool_ratings[-i] = (self.default_mmr *
                        self.default_num_players, self.default_rd *
                        self.default_num_players, self.default_num_players)
# Go through the pool a second time to calculate the new MMR
            for pool_result in pool_results:
                team_id = pool_result[0]
                number_wins = pool_result[1]
                number_losses = pool_result[2]
                number_of_matches = number_wins + number_losses
                (opponent_avg_mmr, opponent_avg_rd) = self.get_opponent_avg(team_id, pool_ratings)
                print(f"Team {team_id} has {number_wins} wins and {number_losses} losses with opponent_avg_mmr of {opponent_avg_mmr} and avg rd of {opponent_avg_rd}")

                #g_rd_j = 1 / (math.sqrt(1 + ((3 * (self.q ** 2) * (opponent_avg_rd ** 2)) / (math.pi ** 2))))
                #print(f"Team {team_id} has G_RDj of {g_rd_j}")
                params = {"team_id": team_id}
                team_players = self.run_sql(self.get_roster_players_sql, params).fetchall()
                for player in team_players:
                    player_id = player[0]
                    #print(f"Getting ratings for player {player_id}")
                    params = {"player_id": player_id}
                    player_rating_result = self.run_sql(self.get_player_current_rating_sql, params).fetchone()
                    player = glicko2.Player()
                    if player_rating_result is not None:
                        (player_mmr, player_rd) = player_rating_result
                        player.setRating(player_mmr)
                        player.setRd(player_rd)
                    # Else use the default mmr 1500 and rd 350
                    
                    opponent_rating_list = []
                    opponent_rd_list = []
                    outcome_list = []
                    for i in range(number_of_matches):
                        opponent_rating_list.append(opponent_avg_mmr)
                        opponent_rd_list.append(opponent_avg_rd)

                    for j in range(number_wins):
                        outcome_list.append(1)

                    for k in range(number_losses):
                        outcome_list.append(0)

                    #print(f"Player {player_id} rating list {opponent_rating_list} outcome
                    player.update_player(opponent_rating_list, opponent_rd_list, outcome_list)

                    print(f"Player {player_id} has current mmr of {player_mmr} and RD of {player_rd}. New MMR: {player.rating} new RD: {player.rd}") 

    def get_opponent_avg(self, team_id, pool_ratings):
        opponent_sum_mmr = 0
        opponent_sum_rd = 0
        opponent_total_players = 0
        #print(f"Getting opponent average for {team_id} with opponent ratings {pool_ratings}")
        for other_team_id, team_totals in pool_ratings.items():
            #print(f"Evaluating opposing team {other_team_id}")
            if team_id != other_team_id:
                (sum_of_team_mmr, sum_of_team_rd, number_of_players) = team_totals
                opponent_sum_mmr = opponent_sum_mmr + sum_of_team_mmr
                opponent_sum_rd = opponent_sum_rd + sum_of_team_rd
                opponent_total_players = opponent_total_players + number_of_players
                #print(f"Added opponent totals for to {opponent_sum_mmr} {opponent_sum_rd} {opponent_total_players}")
        avg_mmr = (opponent_sum_mmr / opponent_total_players)
        avg_rd = (opponent_sum_rd / opponent_total_players)
        print(f"Opponent avg mmr {avg_mmr} and rd {avg_rd} for team {team_id}")
        return (avg_mmr, avg_rd)


if __name__ == '__main__':
    CalculateRatings().run()
