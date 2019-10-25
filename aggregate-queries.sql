 -- 1 Count
 select yearID, count(teamID) as num_teams
 from seanlahman_modeled.teams
 group by yearID
 having yearID > 1920
 order by yearID asc
 -- 2 Min
 select min (f_E) as num_errors, f_GS
 from seanlahman_modeled.player_stats_all
 group by f_GS
 having f_GS > 5
 order by f_GS desc
 -- 3 Max 
 select playerID, max (salary) as max_salary
 from seanlahman_modeled.player_stats_all
 group by playerID
 having max_salary >10000000
 order by max_salary desc
 -- 4 Sum
 
 -- 5 Average
 select yearID, avg (salary) as average_salary from
 seanlahman_modeled.player_stats_all
 group by yearID
 order by yearID
 -- 6 Average
 select playerID, avg (b_R) as average_runs
 from seanlahman_modeled.player_stats_all
 group by playerID
 order by average_runs desc
 
