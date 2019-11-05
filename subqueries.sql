-- 1 selected the first name, last name, and height from the players table where the players' height were greater than the average height across the league 
select nameFirst, nameLast, height
from seanlahman_modeled.players_Beam
where height > (select avg(height) 
  from seanlahman_modeled.players_Beam)

-- 2 selected the number of players from the player stats table where the player played in 2016
select count(playerID) 
from seanlahman_modeled.player_stats_all
where yearID in (select yearID
  from seanlahman_modeled.teams
  where yearID = 2016)

-- 3 join: selecting the playerID, first name, last name, and number of strikeouts from the player stats table joined with the players table where the primary key, pyspID, is bonilbo01200111B
SELECT psa.playerID, pb.nameFirst, pb.nameLast, psa.b_SO
FROM seanlahman_modeled.player_stats_all psa join seanlahman_modeled.players_Beam pb
on psa.playerID = pb.playerID
WHERE psa. pyspID = 
 (SELECT pyspID FROM seanlahman_modeled.player_stats_all 
 where pyspID = "bonilbo01200111B")

-- 4 selected the team names and their average earned runs average where the team played after 2000
SELECT t.name, avg(psa.p_ERA)
FROM seanlahman_modeled.player_stats_all psa join seanlahman_modeled.teams t
on psa.tyID = t.tyID
WHERE psa.tyID in
 (SELECT tyID FROM seanlahman_modeled.teams 
 where yearID > 2000)
group by t.name




