import re
import sqlite3

class Cleanup():

    get_all_tournaments_sql = "SELECT tournament_id, tournament_format, tournament_level, tournament_date FROM tournaments ORDER BY tournament_id"

    get_tournament_teams_sql = \
"""
SELECT t.team_id,
       t.team_name,
       ta.alias_name
  FROM teams t
  LEFT JOIN team_aliases ta
    ON t.team_name = ta.team_name
 WHERE t.tournament_id = :tournament_id
"""

    get_tournament_pool_results_sql = \
"""
WITH 
pool_rows AS (
SELECT tournament_id,
       result_text AS pool_name,
       row_nbr + 1 AS start_row_nbr
  FROM result_text
 WHERE UPPER(result_text) LIKE 'POOL %' 
   AND UPPER(result_text) NOT LIKE 'POOL PLAY%'
   AND tournament_id = :tournament_id 
),
non_pool_rows AS (
SELECT tournament_id,
       row_nbr
  FROM result_text
 WHERE (LENGTH(TRIM(result_text)) <= 1 
    OR UPPER(result_text) LIKE 'SEMI%'
    OR UPPER(result_text) LIKE 'QUARTER%'
    OR UPPER(result_text) LIKE 'FINAL%')
   AND tournament_id = :tournament_id
),
pool_range AS (
SELECT tournament_id,
       pool_name,
       start_row_nbr,
       LEAD(start_row_nbr - 2) OVER (PARTITION BY tournament_id ORDER BY pool_name) AS end_row_nbr
  FROM pool_rows
),
pool_range_fixed AS (
SELECT pr.tournament_id,
       pr.pool_name,
       pr.start_row_nbr,
       CASE WHEN pr.end_row_nbr IS NULL THEN MIN(npr.row_nbr) - 1 ELSE MIN(pr.end_row_nbr) END AS end_row_nbr
  FROM pool_range pr
  LEFT OUTER JOIN non_pool_rows npr
    ON npr.tournament_id = pr.tournament_id
   AND npr.row_nbr > pr.start_row_nbr
 GROUP BY 
       pr.tournament_id,
       pr.pool_name,
       pr.start_row_nbr
)
SELECT rt.tournament_id,
       pr.pool_name,
       rt.result_text
  FROM result_text rt
  JOIN pool_range_fixed pr
    ON pr.tournament_id = rt.tournament_id
   AND rt.row_nbr >= pr.start_row_nbr
   AND (rt.row_nbr <= pr.end_row_nbr OR pr.end_row_nbr IS NULL)
   AND LENGTH(TRIM(rt.result_text)) > 1 
 WHERE rt.tournament_id = :tournament_id
;
"""

    insert_pool_results_sql = \
"""
INSERT INTO pool_results(
  tournament_id,
  pool_name,
  team_id,
  number_wins,
  number_losses
) VALUES (
  :tournament_id,
  :pool_name,
  :team_id,
  :number_wins,
  :number_losses
);
"""

    def run_sql(self, sql, params = {}):
        #print(f"Running sql {sql}")
        cursor_obj = self.connection_obj.cursor()
        return cursor_obj.execute(sql, params)

    def run(self):
        self.connection_obj = sqlite3.connect('yankee_mmr.db')
        try:
            #tournaments = self.run_sql(self.get_all_tournaments_sql).fetchone()
            tournaments = self.run_sql(self.get_all_tournaments_sql).fetchall()
            num_tournaments = len(tournaments)
            i = 0 
            for tournament in tournaments:
                self.cleanup_pool_results(tournament)
                print(f"Completed {i+1} of {num_tournaments} ({((i+1) / num_tournaments) * 100}%)")
                i = i + 1
            self.connection_obj.commit()
        finally:
            self.connection_obj.close()


    def cleanup_pool_results(self, tournament):
        (tournament_id, tournament_format, tournament_level, tournament_date) = tournament
        print(f"Cleaning up results for {tournament_id}: {tournament_format} {tournament_level} {tournament_date}")
        params = {"tournament_id": tournament_id}

        team_name_results = self.run_sql(self.get_tournament_teams_sql, params).fetchall()
        team_recaps = self.run_sql(self.get_tournament_pool_results_sql, params).fetchall()
        for team_recap in team_recaps:
            team_id = None
            number_wins = None
            number_losses = None
            (tournament_id, pool_name, recap_text) = team_recap
            record_last_matches = re.search(r"([0-9]+\.)?\s?(.*)\s([0-9]+)-([0-9]+)", recap_text)
            if record_last_matches is not None:
                #print(f"results last: {recap_text}")
                record_team_name = record_last_matches.group(2)
                number_wins = record_last_matches.group(3)
                number_losses = record_last_matches.group(4)
            else: 
                record_first_matches = re.search(r"([0-9]+)-([0-9]+)?\s?(.*)", recap_text)
                if record_first_matches is not None:
                    #print(f"results first: {recap_text}")
                    record_team_name = record_first_matches.group(3)
                    number_wins = record_first_matches.group(1)
                    number_losses = record_first_matches.group(2)

            #print(f"Result text {recap_text}: {record_team_name}")
            #print(f"team {team_result}")
            for team_name_result in team_name_results:
                (potential_team_id, potential_team_name, potential_alias_name) = team_name_result
                #print(f"Testing team name {potential_team_name} against {team_result}")
                if re.search(record_team_name, potential_team_name, re.IGNORECASE):
                    team_id = potential_team_id
                    team_name = potential_team_name
                    team_name_results.remove(team_name_result)
                    break
                elif potential_alias_name is not None and re.search(potential_alias_name, recap_text, re.IGNORECASE):
                    team_id = potential_team_id
                    team_name = potential_team_name
                    team_name_results.remove(team_name_result)
                    break

            if team_id is not None:
            #    print(f"Found {team_name} in {team_result}")
                #print(f"Team {team_name} had record {number_wins}-{number_losses}") 
                params = {"tournament_id": tournament_id, "pool_name": pool_name, "team_id": team_id, "number_wins": number_wins, "number_losses": number_losses}
                self.run_sql(self.insert_pool_results_sql, params)
                pool_results_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
            #else: 
                #print(f"Could not find a roster for result {result_text} in {team_name_results}")

        #if len(team_name_results) > 0:
            #print(f"{tournament_format} {tournament_level} {tournament_date}")
            #print(f"Remaining rosters with no results {team_name_results}")
            #print(f"Teams with records {team_recaps}")

if __name__ == '__main__':
    Cleanup().run()
