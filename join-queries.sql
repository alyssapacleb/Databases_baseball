-- 1: selected distinct pairs of players, years, and positions who played both first base and out field after 2015 and started in more than 60 games 
select DISTINCT f.playerID, f.yearID, f.POS, fi.yearID as year2, fi.POS as pos2
from seanlahman_staging.Fielding f
join seanlahman_staging.Fielding fi on f.playerID = fi.playerID
where f.POS = '1B' and fi.POS = 'OF' and f.yearID > 2015 and fi.yearID > 2015 and f.GS > 60

-- 2: selected the player ID from the pitching file where the player switched teams in 2017 from the Detroit Tigers to the Houston Astros and allowed less than 5 runs in 2017. 
select p.playerID
from seanlahman_staging.Pitching p
join seanlahman_staging.Pitching pi on p.playerID = pi.playerID
where p.stint = 1 and p.teamID = 'DET' and pi.stint = 2 and pi.teamID = 'HOU' and p.yearID = 2017 and pi.R <5

-- 3: selected the first and last name, salary, year, and team of players who made more than $30,000,000 after 2015 and ordered the results in descending order. 
select p.nameFirst, p.nameLast, s.salary, s.yearID, s.teamID 
from seanlahman_staging.People p join seanlahman_staging.Salaries s 
on p.playerID = s.playerID 
where s.salary > 30000000 and s.yearID > 2015 order by s.salary DESC

-- 4: selected all distinct players and the position they played from the fielding file with their corresponding player IDs when the player was born on August 19th and played after 2000. 
select distinct f.POS, p.nameFirst, p.nameLast
from seanlahman_staging.Fielding f left outer join seanlahman_staging.People p
on f.playerID = p.playerID
where f.yearID > 2000 and p.birthMonth = 8 and p.birthday = 19 

-- 5: selected the pitcher's opponent's batting average and the pitcher's name from the Pitching and People files where the opponent's batting average was not recorded and the player was pitching after 2015. 
select distinct pi.BAOpp, pe.nameFirst, pe.nameLast
from seanlahman_staging.Pitching pi right outer join seanlahman_staging.People pe
on pi.playerID = pe.playerID
where pi.BAOpp is null and pi.yearID > 2015

-- 6: selected the runs batted in from the Batting file, the first and last names of the players from the People file and right joined the tables where the players played in 1884 and their runs batted in were not recorded
select distinct b.RBI, p.nameFirst, p.nameLast
from seanlahman_staging.Batting b right outer join seanlahman_staging.People p
on b.playerID = p.playerID
where b.RBI is null and b.yearID = 1884
