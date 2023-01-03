import re
import sqlite3

class CalculateRatings():

    default_mmr = 1500

    default_rd = 350

    default_num_players = 6 

    q = (ln(10) / 400)

    get_tournaments_sql = "SELECT tournament_id, tournament_format, tournament_level, tournament_date FROM tournaments WHERE tournament_id = 1 ORDER BY tournament_date, tournament_format, tournament_level, tournament_id"

    get_tournaments_pools_sql = "SELECT DISTINCT pool_name FROM pool_results WHERE tournament_id = :tournament_id"

    get_tournaments_pool_results_sql = "SELECT team_id, number_wins, number_losses FROM pool_results WHERE tournament_id = :tournament_id AND pool_name = :pool_name"

    get_roster_rating_sql = "SELECT r.player_id, pm.mmr_rating, pm.rd FROM rosters r JOIN player_mmr pm ON r.player_id = pm.player_id WHERE team_id = :team_id"

    def run_sql(self, sql, params = {}):
        #print(f"Running sql {sql}")
        cursor_obj = self.connection_obj.cursor()
        return cursor_obj.execute(sql, params)

    def run(self):
        print(f"q is {q}")
        self.connection_obj = sqlite3.connect('yankee_mmr.db')
        try:
            tournaments_to_rate = self.run_sql(self.get_tournaments_sql).fetchall()
            num_tournaments = len(tournaments)
            i = 0 
            for tournament in tournaments:
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
            pool_name = pools[0]
            print(f"Rating pool {pool_name} in tournament {tournament_id}")
            params = {"tournament_id": tournament_id, "pool_name": pool_name}
            pool_results = self.run_sql(self.get_tournament_pool_results_sql, params).fetchall()
            pool_ratings = {}
            # Go through the pool once to calculate average mmr and rd
            for pool_result in pool_results:
                team_id = pool_result[0]
                # We just need a sample of wins and losses to figure out how many teams were played
                sample_wins = pool_result[1]
                sample_losses = pool_result[2]
                params = {"team_id": team_id}
                roster_ratings = self.run_sql(self.get_roster_ratings_sql, params).fetchall()
                sum_of_team_mmr = 0
                sum_of_team_rd = 0
                number_of_players = 0
                for roster_rating in roster_ratings:
                    player_id = roster_rating[0]
                    player_mmr = roster_rating[1]
                    player_rd = roster_rating[2]
                    sum_of_team_mmr = sum_of_team_mmr + player_mmr
                    sum_of_team_rd = sum_of_team_rd + player_rd
                    number_of_players = number_of_players + 1
                pool_ratings[team_id] = (sum_of_team_mmr, sum_of_team_rd, number_of_players)

            # Add missing teams with default ratings
            number_of_expected_teams = (sample_wins + sample_losses) / 2
            missing_teams = number_of_expected_teams - length(pool_rating)
            for i in range(missing_teams):
                pool_ratings[-i] = (self.default_mmr * self.default_num_players, self.default_rd * self.default_num_players, self.default_num_players)

            # Go through the pool a second time to calculate the new MMR
            for pool_result in pool_results:
                team_id = pool_result[0]
                number_wins = pool_result[1]
                number_losses = pool_result[2]
                (opponent_avg_mmr, opponent_avg_rd) = self.get_opponent_avg(team_id, pool_ratings)

                # do something

    def get_opponent_avg(self, team_id, pool_ratings):
        opponent_sum_mmr = 0
        opponent_sum_rd = 0
        opponent_total_players = 0
        for other_team_id, team_totals in pool_ratings.items():
            if team_id != other_team_id:
                (sum_of_team_mmr, sum_of_team_rd, number_of_players) = team_totals
                opponent_sum_mmr = opponent_sum_mmr + sum_of_team_mmr
                opponent_sum_rd = opponent_sum_rd + sum_of_team_rd
                opponent_total_players = opponent_total_players
        avg_mmr = (opponent_sum_mmr / opponent_total_players)
        avg_rd = (opponent_sum_rd / opponet_total_players)
        return (avg_mmr, avg_rd)


if __name__ == '__main__':
    CalculateRatings().run()
