-- Created teams table
create table seanlahman_modeled.teams as
select distinct teamID, lgID, yearID, name, Rank
from seanlahman_staging.Teams
order by teamID

-- Created teams primary key
UPDATE seanlahman_modeled.teams
SET tyID = concat(cast(teamID as string), cast(yearID as string))
WHERE TRUE

-- Created player table
create table seanlahman_modeled.players as
select distinct playerID, nameFirst, nameLast, nameGiven, debut, weight, height, birthCountry, birthCity, birthyear, birthmonth, birthday, bats, throws
from seanlahman_staging.People
order by playerID

-- Created pitchers table
create table seanlahman_modeled.pitchers as
select distinct playerID, yearID, stint, teamID, lgID, W, L, G, GS, CG, SHO, SV, IPouts, H, ER, HR, BB, SO, safe_cast(BAOpp as FLOAT64) as BAOpp, ERA, safe_cast(IBB as INT64) as IBB, WP, safe_cast(HBP as INT64) as HBP, BK, BFP, R, GF, safe_cast(SH as INT64) as SH , safe_cast(SF as INT64) as SF, safe_cast(GIDP as INT64) as GIDP
from seanlahman_staging.Pitching
order by playerID

-- Add pitchers primary key
UPDATE seanlahman_modeled.pitchers
SET pysID = concat(cast(playerID as string), cast(yearID as string), cast(stint as string))
WHERE TRUE

-- Created batters table
create table seanlahman_modeled.batters as
select distinct playerID, yearID, stint, teamID, lgID, G, AB, R, H, _2B as B2, _3B as B3, HR, RBI, SB, CS, BB, SO, safe_cast(IBB as INT64) as IBB, safe_cast(HBP as INT64) as HBP, safe_cast(SH as INT64) as SH, safe_cast(SF as INT64) as SF, GIDP
from seanlahman_staging.Batting
order by playerID

-- Created batters primary key
UPDATE seanlahman_modeled.batters
SET pysID = concat(cast(playerID as string), cast(yearID as string), cast(stint as string))
WHERE TRUE

-- Created fielders table
create table seanlahman_modeled.fielders as
select distinct playerID, yearID, stint, teamID, lgID, POS, G, GS, InnOuts, PO, A, E, DP, safe_cast(WP as INT64) as WP, SB, CS, safe_cast(ZR as INT64) as ZR
from seanlahman_staging.Fielding
order by playerID

-- Created fielders primary key
UPDATE seanlahman_modeled.fielders
SET pysID = concat(cast(playerID as string), cast(yearID as string), cast(stint as string))
WHERE TRUE

-- Created salaries table
create table seanlahman_modeled.salaries as
select distinct playerID, yearID, teamID, lgID, salary
from seanlahman_staging.Salaries
order by playerID

-- Created salaries primary key
UPDATE seanlahman_modeled.salaries
SET pysID = concat(cast(playerID as string), cast(yearID as string))
WHERE TRUE

-- PRIMARY KEY CHECK
select count (*) from seanlahman_modeled.teams
select count (*) from seanlahman_modeled.fielders 
select count (*) from seanlahman_modeled.batters
select count (*) from seanlahman_modeled.players 
select count (*) from seanlahman_modeled.pitchers
select count (*) from seanlahman_modeled.salaries

select count (distinct tyID) from seanlahman_modeled.teams
select count (distinct pysID) from seanlahman_modeled.fielders 
select count (distinct pysID) from seanlahman_modeled.batters
select count (distinct playerID) from seanlahman_modeled.players
select count (distinct playerID) from seanlahman_modeled.pitchers
select count (distinct playerID) from seanlahman_modeled.salaries






