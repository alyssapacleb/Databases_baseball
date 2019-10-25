 -- 1 Count: Select every year of baseball since 1920 and display how many teams were in the entire league in descneding order.
 -- We foudn that there were much fewer teams in the baseball decades ago.
 select yearID, count(teamID) as num_teams
 from seanlahman_modeled.teams
 group by yearID
 having yearID > 1920
 order by yearID asc
 -- 2 Min: Select the minimum number or errors that players have made, but only incude players that have started a minimum of 5 games.
 -- Then, display then by the number of games started in descending order.
 select min (f_E) as num_errors, f_GS
 from seanlahman_modeled.player_stats_all
 group by f_GS
 having f_GS > 5
 order by f_GS desc
 -- 3 Max: Select the player IDs and the top salaries in the league. Then, only show players that have salaries that are greater than 1 million.
 -- Finally, order these salaries in descending order so that we can see the top salaries first.
 select playerID, max (salary) as max_salary
 from seanlahman_modeled.player_stats_all
 group by playerID
 having max_salary >10000000
 order by max_salary desc
 -- 4 Sum: Select all of the players' IDs and the sum of their total strike outs. Then, display each player with their number of strikeouts. 
 select playerID, sum (p_SO) as num_total_SO
 from seanlahman_modeled.player_stats_all
 group by playerID
 -- 5 Average: Select the year and the average salary for baseball players accross the league. Then, we show each year and it's corresponding average salary.
 select yearID, avg (salary) as average_salary from
 seanlahman_modeled.player_stats_all
 group by yearID
 order by yearID
 -- 6 Average: Select the players' IDs and the average number of runs that they each have. Then display the player IDs beside the average number of runs that they each had.
 select playerID, avg (b_R) as average_runs
 from seanlahman_modeled.player_stats_all
 group by playerID
 order by average_runs desc
 
