-- Find the number of rows in each dataset to determine if any file contains a primary key

select count (*) from seanlahman_staging.Appearances --105789

select count (*) from seanlahman_staging.Fielding --140921

select count (*) from seanlahman_staging.People --19617

select count (*) from seanlahman_staging.Pitching --46699

select count (*) from seanlahman_staging.Salaries --26428

-- This query shows that the playerID field is a primary key because each player is distinct in the People file. 

select count (distinct playerID ) from seanlahman_staging.People

-- These queries showed that the identifiers in other files were not primary keys and were, instead, foreign keys. These queries also showed their many to  many relationship with each other. 

select count (distinct yearID ) from seanlahman_staging.Appearances
select count (distinct teamID ) from seanlahman_staging.Appearances
select count (distinct lgID ) from seanlahman_staging.Appearances
select count (distinct playerID ) from seanlahman_staging.Appearances

select count (distinct yearID ) from seanlahman_staging.Batting
select count (distinct teamID ) from seanlahman_staging.Batting
select count (distinct lgID ) from seanlahman_staging.Batting
select count (distinct playerID ) from seanlahman_staging.Batting

select count (distinct yearID ) from seanlahman_staging.Fielding
select count (distinct teamID ) from seanlahman_staging.Fielding
select count (distinct lgID ) from seanlahman_staging.Fielding
select count (distinct playerID ) from seanlahman_staging.Fielding

select count (distinct yearID ) from seanlahman_staging.Pitching
select count (distinct teamID ) from seanlahman_staging.Pitching
select count (distinct lgID ) from seanlahman_staging.Pitching
select count (distinct playerID ) from seanlahman_staging.Pitching

select count (distinct yearID ) from seanlahman_staging.Salaries
select count (distinct teamID ) from seanlahman_staging.Salaries
select count (distinct lgID ) from seanlahman_staging.Salaries
select count (distinct playerID ) from seanlahman_staging.Salaries
