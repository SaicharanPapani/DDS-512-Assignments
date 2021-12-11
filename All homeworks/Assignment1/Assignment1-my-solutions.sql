
/*
Creating the table and loading the dataset
*/
DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings (userid INT, temp1 VARCHAR(10),  movieid INT , temp3 VARCHAR(10),  rating REAL, temp5 VARCHAR(10), timestamp INT);
COPY ratings FROM 'test_data1.txt' DELIMITER ':';
ALTER TABLE ratings DROP COLUMN temp1, DROP COLUMN temp3, DROP COLUMN temp5, DROP COLUMN timestamp;

-- Do not change the above code except the path to the dataset.
-- make sure to change the path back to default provided path before you submit it.

-- Part A
/* Write the queries for Part A*/
SELECT * FROM ratings;
SELECT * FROM ratings WHERE rating BETWEEN 3 AND 5;
SELECT userid, movieid FROM ratings WHERE rating BETWEEN 2 AND 5;
SELECT DISTINCT rating From ratings;
SELECT * FROM ratings WHERE rating IN (1,2,3);

-- Part B
/* Create the fragmentations for Part B1 */
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS f3;
CREATE TABLE f1 AS SELECT * FROM ratings WHERE rating BETWEEN 0 AND 3;
CREATE TABLE f2 AS SELECT * FROM ratings WHERE rating BETWEEN 2 AND 4;
CREATE TABLE f3 AS SELECT * FROM ratings WHERE rating BETWEEN 3 AND 5;

/* Write reconstruction query/queries for Part B1 */
-- Reconstructed the table using fragments
DROP TABLE IF EXISTS ratingsduplicate;
CREATE TABLE ratingsduplicate AS
SELECT * FROM f1
UNION
SELECT * FROM f2
UNION
SELECT * FROM f3;

-- comparing the original and reconstructed table 
-- this is not necessary but performing it.
SELECT * FROM ratings
EXCEPT
SELECT * FROM ratingsduplicate;

/* Write your explanation as a comment */
/* Fragments:
	f1: 0 <= rating <= 3
	f2: 2 <= rating <= 4
	f3: 3 <= rating <= 5
	
	Completeness: Decomposition of relation R into fragments R1, R2, ..., Rn is complete if and only if each data item in R 
	can also be found in some Ri
		So every row in the ratings table is present in some fragment.
        Hence it satisfies the completeness
	Recontruction: If relation R  is decomposed into fragments R1, R2, ..., Rn, then there should exist some relational operator 
	∇ such that R = ∇1≤i≤nRi
		When you merge all the fragments (f1,f2,f3) using union operator, 
        We get the original table back(ratings).
		Hence it satisfies reconstruction.
	Disjointness: If relation R is decomposed into fragments R1, R2, ..., Rn, and data item di is in Rj, 
	then di should not be in any other fragment Rk (k ≠ j ).
		The items with rating 2 are present in f1 and f2. 
        So, when we perform intersection, we get few records and it deviates the disjointness.
        Hence, it doesn't satisfy the disjointness.
*/

/* Create the fragmentations for Part B2 */
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS f3;
CREATE TABLE f1 AS SELECT movieid FROM ratings;
CREATE TABLE f2 AS SELECT userid FROM ratings;
CREATE TABLE f3 AS SELECT rating from ratings;

/* Write your explanation as a comment */
/* Performed a vertical fragmentation
   f1: Contains movieid 
   f2: Contains userid
   f3: Conatins rating

   Completeness: Decomposition of relation R into fragments R1, R2, ..., Rn is complete if and 
   only if each data item in R can also be found in some Ri
		All movieid column data items are present in f1
        All userid column values are present in f2.
		All rating column values are present in f3
        So all data items are present in the formed fragments.
        Hence it satified Completeness.
	Disjointness: If relation R is decomposed into fragments R1, R2, ..., Rn, and data item di is in Rj, 
	then di should not be in any other fragment Rk (k ≠ j ).
		There are no common columns in f1, f2 and f3, so no data item of f1 is present in f2. 
        Hence it satisfied disjointness.
    Recontruction: If relation R is decomposed into fragments R1, R2, ..., Rn, then 
    there should exist some relational operator ∇ such that R = ∇1≤i≤nRi
        As there is no common attribute which can act as a join attribute, we cannot join the fragments 
        and get back the original table.
        Hence it doesn't satify the reconstruction.
*/


/* Create the fragmentations for Part B3 */
-- rating are from 0 to 5 with incrementing value of 0.5
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS f3;
CREATE TABLE f1 AS SELECT * FROM ratings WHERE rating BETWEEN 0 AND 2;
CREATE TABLE f2 AS SELECT * FROM ratings WHERE rating BETWEEN 2.5 AND 4;
CREATE TABLE f3 AS SELECT * FROM ratings WHERE rating BETWEEN 4.5 AND 5;

/* Write reconstruction query/queries for Part B3 */
-- Reconstructed the table using fragments
DROP TABLE IF EXISTS ratingsduplicate;
CREATE TABLE ratingsduplicate AS
SELECT * FROM f1
UNION
SELECT * FROM f2
UNION
SELECT * FROM f3;

-- comparing the original and reconstructed table 
-- this is not necessary but performing it.
SELECT * FROM ratings
EXCEPT
SELECT * FROM ratingsduplicate;

/* Write your explanation as a comment */
/* Fragments:
	f1: 0 <= rating <= 2
	f2: 2.5 <= rating <= 4
	f3: 4.5 <= rating <= 5
	
	Completeness: Decomposition of relation R into fragments R1, R2, ..., Rn is complete if and only 
    if each data item in R can also be found in some Ri
		So every row in the ratings table is present in some fragment.
        Hence it satisfy completeness
	Recontruction: If relation R is decomposed into fragments R1, R2, ..., Rn, 
    then there should exist some relational operator ∇ such that R = ∇1≤i≤nRi
		When we merge all the fragments (f1,f2,f3) using union, 
        We are getting the original table back(ratings).
		Hence it satisfy reconstruction
	Disjointness: If relation R is decomposed into fragments R1, R2, ..., Rn, and data item di is in Rj, 
	then di should not be in any other fragment Rk (k ≠ j ).
		No item with is present in multiple fragments. 
        example: rating 2 is only present in f1 and not available in any other fragment.
        Hence it satisfy disjointness.
*/


-- Part C
/* Write the queries for Part C */
-- 5 queries on f1
SELECT * FROM f1;
SELECT * FROM f1 WHERE rating BETWEEN 3 AND 5;
SELECT userid, movieid FROM f1 WHERE rating BETWEEN 2 AND 5;
SELECT DISTINCT rating From f1;
SELECT * FROM f1 WHERE rating IN (0,0.5,1,1.5,2);

-- 5 queries on f2
SELECT * FROM f2;
SELECT * FROM f2 WHERE rating BETWEEN 3 AND 5;
SELECT userid, movieid FROM f2 WHERE rating BETWEEN 2 AND 5;
SELECT DISTINCT rating From f2;
SELECT * FROM f2 WHERE rating IN (0,0.5,1,1.5,2);


-- 5 queries on f3
SELECT * FROM f3;
SELECT * FROM f3 WHERE rating BETWEEN 3 AND 5;
SELECT userid, movieid FROM f3 WHERE rating BETWEEN 2 AND 5;
SELECT DISTINCT rating From f3;
SELECT * FROM f3 WHERE rating IN (0,0.5,1,1.5,2);
