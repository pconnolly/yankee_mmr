import re
import sqlite3

class ParseRecap():

    get_all_tournaments_sql = "SELECT tournament_id, tournament_format, tournament_level, tournament_date FROM tournaments ORDER BY tournament_id"

    get_tournament_teams_sql = \
"""
SELECT t.team_id,
       t.team_name
  FROM teams t
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
   AND tournament_id = :tournament_id
),
non_pool_rows AS (
SELECT tournament_id,
       row_nbr
  FROM result_text
 WHERE (LENGTH(TRIM(result_text)) <= 1
    OR UPPER(result_text) LIKE 'SEMI%'
    OR UPPER(result_text) LIKE 'QUARTER%'
    OR UPPER(result_text) LIKE '%QUARTERFINALS%'
    OR UPPER(result_text) LIKE 'QUATER%' --Typo on 2021-10-24 M C+
    OR UPPER(result_text) LIKE '\_\_\_\_\_\_%'
    OR UPPER(result_text) LIKE 'POOL %'
    OR UPPER(result_text) LIKE '%FINAL%')
   AND tournament_id = :tournament_id
),
pool_range AS (
SELECT pr.tournament_id,
       pr.pool_name,
       pr.start_row_nbr,
       COALESCE(MIN(npr.row_nbr) - 1, (SELECT MAX(row_nbr) FROM result_text rt WHERE pr.tournament_id = rt.tournament_id)) end_row_nbr
  FROM pool_rows pr
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
  JOIN pool_range pr
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

    get_tournament_match_results_sql = \
"""
WITH defeat_text AS (
  SELECT 'D.' defeat_text 
  UNION ALL 
  SELECT 'DEF' 
  UNION ALL 
  SELECT 'DEFEATED') 
SELECT rt.tournament_id,
       rt.result_text 
  FROM result_text rt 
  JOIN defeat_text dt 
    ON UPPER(rt.result_text) LIKE '% ' || dt.defeat_text || ' %' 
 WHERE tournament_id = :tournament_id
;"""

    insert_match_results_sql = \
"""
INSERT INTO match_results(
  tournament_id,
  winning_team_name,
  winning_team_id,
  losing_team_name,
  losing_team_id,
  match_description
) VALUES (
  :tournament_id,
  :winning_team_name,
  :winning_team_id,
  :losing_team_name,
  :losing_team_id,
  :match_description
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
                (tournament_id, tournament_format, tournament_level, tournament_date) = tournament
                print(f"Cleaning up results for {tournament_id}: {tournament_format} {tournament_level} {tournament_date}")
                self.cleanup_pool_results(tournament)
                self.cleanup_match_results(tournament)
                print(f"Completed {i+1} of {num_tournaments} ({((i+1) / num_tournaments) * 100}%)")
                i = i + 1
            self.connection_obj.commit()
        finally:
            self.connection_obj.close()

    def cleanup_match_results(self, tournament):
        (tournament_id, tournament_format, tournament_level, tournament_date) = tournament
        params = {"tournament_id": tournament_id}
        match_recaps = self.run_sql(self.get_tournament_match_results_sql, params).fetchall()
        for match_recap in match_recaps:
            (tournament_id, recap_text) = match_recap
            #match_recap_matches = re.search(r"(Quarter:|Quarterfinal:|Semi:|Semi-finals:|Semi-Finals:|Finals:)?\s?(.*)\s(d\.|def|def\.|defeated)\s(.s*)\s([0-9]+)-([0-9]+)", recap_text)
            #match_recap_matches = re.search(r"(Quarter:|Quarterfinal:|Semi:|Semi-finals:|Semi-Finals:|Finals:)?\s?(.*)(d.)(.*)\s(([0-9]+)-([0-9]+)\s?,?)*", recap_text)
            match_recap_matches = re.search(r"(Quarter.*:|Semi.*:|Finals\s*:)?\s?(.*)\s(D\.|d\.|def|Defeated|defeated|DEFEATED|DEF)\s(.*)", recap_text)
            if match_recap_matches is not None:
                match_name = match_recap_matches.group(1).strip(":") if match_recap_matches.group(1) is not None else None
                winning_team_name = match_recap_matches.group(2).strip()
                losing_team_name_with_record = match_recap_matches.group(4)
                # I couldn't get the regex right, so we reverse the string to strip off the trailing record and then reverse it again
                losing_team_name = re.search(r"(\s?,?([0-9]+)-([0-9]+))?(\s?,?([0-9]+)-([0-9]+))?(\s?,?([0-9]+)-([0-9]+))?(.*)", losing_team_name_with_record[::-1]).group(10)[::-1].strip()
                winning_team_id = self.find_team_id(tournament_id, winning_team_name)
                losing_team_id = self.find_team_id(tournament_id, losing_team_name)
                
                params = {"tournament_id": tournament_id, "winning_team_name": winning_team_name, "winning_team_id": winning_team_id, "losing_team_name": losing_team_name, "losing_team_id": losing_team_id, "match_description": match_name}
                self.run_sql(self.insert_match_results_sql, params)
                match_results_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]

                
                #print(f"Recap text {recap_text}")
                #print(f"Match Name: {match_name} Winning Team: {winning_team_id} {winning_team_name} Losing Team: {losing_team_id} {losing_team_name}")
            else:
                print(f"Unabled to determine results for match {recap_text}")

    def cleanup_pool_results(self, tournament):
        (tournament_id, tournament_format, tournament_level, tournament_date) = tournament
        params = {"tournament_id": tournament_id}

        team_recaps = self.run_sql(self.get_tournament_pool_results_sql, params).fetchall()
        for team_recap in team_recaps:
            team_id = None
            record_team_name = None
            number_wins = None
            number_losses = None
            (tournament_id, pool_name, recap_text) = team_recap

            #print(f"Pool {pool_name} text {recap_text}")
            record_last_matches = re.search(r"([0-9]+\.)?\s?(.*)\s[-–]?\s?([0-9]+)\s?[-–]\s?([0-9]+)", recap_text)
            if record_last_matches is not None:
                record_team_name = record_last_matches.group(2)
                number_wins = record_last_matches.group(3)
                number_losses = record_last_matches.group(4)
                #print(f"results last: {recap_text} found team name {record_team_name} with {number_wins} wins and {number_losses} losses")
            else: 
                record_first_matches = re.search(r"([0-9]+)\s?[-–]\s?([0-9]+)?\s?(.*)", recap_text)
                if record_first_matches is not None:
                    record_team_name = record_first_matches.group(3)
                    number_wins = record_first_matches.group(1)
                    number_losses = record_first_matches.group(2)
                    #print(f"results first: {recap_text} found team name {record_team_name} with {number_wins} wins and {number_losses} losses")
            if record_team_name is not None:
                team_id = self.find_team_id(tournament_id, record_team_name)
                if team_id is not None:
                #    print(f"Found {team_name} in {team_result}")
                    #print(f"Team {team_name} had record {number_wins}-{number_losses}") 
                    params = {"tournament_id": tournament_id, "pool_name": pool_name, "team_id": team_id, "number_wins": number_wins, "number_losses": number_losses}
                    self.run_sql(self.insert_pool_results_sql, params)
                    pool_results_id = self.run_sql("SELECT last_insert_rowid()").fetchone()[0]
            else: 
                print(f"Could not parse pool results for tournament {tournament_id} {tournament_format} {tournament_level} {tournament_date} {recap_text}")

    def find_team_id(self, tournament_id, record_team_name):
        #print(f"Result text {recap_text}: {record_team_name}")
        #print(f"team {team_result}")
        params = {"tournament_id": tournament_id}
        team_name_results = self.run_sql(self.get_tournament_teams_sql, params).fetchall()
        for team_name_result in team_name_results:
            (potential_team_id, potential_team_name) = team_name_result
            #print(f"Testing team name {potential_team_name} against {record_team_name}")
            try:
                if re.search(record_team_name, potential_team_name, re.IGNORECASE):
                    return potential_team_id
            except:
                pass
        #print(f"Could not find a roster for result {record_team_name} in {team_name_results}")


if __name__ == '__main__':
    ParseRecap().run()
