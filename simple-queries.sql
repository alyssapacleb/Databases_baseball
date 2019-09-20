-- selected the cities where players were born and died as well as their player ID from the People file when the player died in 1972 in Texas and ordered the players in ascending order by the year they were born
SELECT birthCity, deathCity, playerID FROM seanlahman_staging.People 
where deathYear = 1972 and deathState = 'TX'
order by birthYear

-- selected all rows from the Salaries file where the team ID is TEX (Texas Rangers based in Dallas) and ordered the players in descending order by salary
SELECT * FROM seanlahman_staging.Salaries 
WHERE teamID = 'TEX' 
ORDER BY salary DESC

-- selected all rows from the Pitching file where the pitchers from the Houston Astros allowed less than 5 runs per game on average after entering the American League and ordered by the number of wins in descending order 
SELECT * FROM seanlahman_staging.Pitching 
WHERE ERA < 5.00 and teamID = 'HOU' and lgID = 'AL'
ORDER BY W DESC

-- selected all rows from the Appearances file where the players started in 162 games (every game in a baseball season) and ordered by the year in descending order
SELECT * FROM seanlahman_staging.Appearances 
WHERE GS = 162.00
ORDER BY yearID DESC

-- selected the players and teams from the Batting file where the player played in the American League ordered by the number of home runs in descending order. 
SELECT playerID, teamID FROM seanlahman_staging.Batting 
WHERE lgID = 'AL'
ORDER BY HR DESC

-- selected all rows from the Fielding file where the player played at the first base and ordered by the number of errors in descending order.
SELECT * FROM seanlahman_staging.Fielding
WHERE POS = '1B'
ORDER BY E DESC

-- selected the names, home runs, and years from the Teams file where the team has more wins than loses and their attendance is greater than 1,000,000 and ordered by attendance in descending order. 
SELECT name, HR, yearID FROM seanlahman_staging.Teams 
WHERE W > L and attendance > 1000000
ORDER BY attendance DESC
