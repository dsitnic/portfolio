--------- DATA CLEANING ----------------
SELECT *
FROM layoffs
LIMIT 100;

-- remove duplicates
-- standardize data
-- data types
-- null values or blank values --> populate or remove
-- remove any columns not needed

-- create a temp table to work with
CREATE TABLE layoffs_temp AS
SELECT *
FROM layoffs;

-- get overview
PRAGMA table_info(layoffs);

-- find duplicates with a window function
WITH cte AS
(
SELECT
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions) AS row_num,
	*
FROM layoffs_temp
)
SELECT *
FROM cte
WHERE cte.row_num > 1;

-- we can also find duplicates with a group by
SELECT company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions, COUNT(*) AS row_count
FROM layoffs_temp
GROUP BY company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions
HAVING count(*) > 1;

-- create a new table with row_num as column in order to filter it out
CREATE TABLE layoff_without_dupl AS
SELECT
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions) AS row_num,
	*
FROM layoffs;

-- another possibility
CREATE TABLE layoff_without_dupl AS
SELECT company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions, COUNT(*) AS row_count
FROM layoffs_temp
GROUP BY company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions
HAVING count(*) = 1;


-- now we can filter out duplicates
SELECT *
FROM layoff_without_dupl
WHERE row_num = 1;

-- lets have a look on the important columns onw after another
SELECT DISTINCT(company)
FROM layoff_without_dupl
ORDER BY company;
-- there are spaces at some entries
-- lets get rid of them with TRIM
SELECT DISTINCT(TRIM(company))
FROM layoff_without_dupl
ORDER BY company;
-- update the column
UPDATE layoff_without_dupl
SET company = TRIM(company);
-- check if it was done
SELECT DISTINCT(company)
FROM layoff_without_dupl
WHERE company LIKE " %";

-- go to next column
SELECT DISTINCT location
FROM layoff_without_dupl
ORDER BY location;
-- Düsseldorf is two times one time with ü and u available
SELECT *
FROM layoff_without_dupl
WHERE location LIKE "Düss%";
-- update with ue
UPDATE layoff_without_dupl
SET location = "Duesseldorf"
WHERE location LIKE "Düssel%"
	OR location LIKE "Dussel%"

-- next column
SELECT distinct industry
FROM layoff_without_dupl
ORDER BY industry;

-- Different Crypto strings
UPDATE layoff_without_dupl
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

-- NULL and Empty cell also there
-- location looks good
SELECT distinct location
FROM layoff_without_dupl
ORDER BY location;

SELECT distinct country
FROM layoff_without_dupl
ORDER BY country;

UPDATE layoff_without_dupl
SET country = "United States"
WHERE country LIKE "United States%";

-- rename the current table in order to create a new one with that name
ALTER TABLE layoff_without_dupl RENAME TO layoff_without_dupl_old;

-- create it again with correct datatypes
-- get rid of colum row_num
	-- als possible with 
	-- ALTER TABLE layoff_without_dupl DROP COLUMN row_num;
-- rename column date to date_layoff
	-- also possible with
	-- ALTER TABLE layoff_without_dupl RENAME COLUMN "date" TO date_layoff;
CREATE TABLE layoff_without_dupl (
    company TEXT,
    location INTEGER,
    industry TEXT,
    total_laid_off INTEGER,
    percentage_laid_off INTEGER,
    date_layoff DATE,
    stage TEXT,
    country TEXT,
    funds_raised_millions INTEGER
);

-- insert the data to the table
INSERT INTO layoff_without_dupl (company, location, industry, total_laid_off, percentage_laid_off, date_layoff, stage, country, funds_raised_millions)
SELECT company, location, industry, total_laid_off, percentage_laid_off, "date", stage, country, funds_raised_millions
FROM layoff_without_dupl_old;

-- NULL became a string because we converted column to integer
SELECT *
FROM layoff_without_dupl
WHERE total_laid_off = "NULL";

----- lets look at NULLs and blank values
SELECT *
FROM layoff_without_dupl
WHERE 
	company IS NULL
	OR location IS NULL
	OR industry IS NULL
	OR total_laid_off IS NULL
	OR percentage_laid_off IS NULL
	OR date_layoff IS NULL
	OR stage IS NULL
	OR country IS NULL
	OR funds_raised_millions IS NULL;

SELECT *
FROM layoff_without_dupl
WHERE 
	company = "NULL"
	OR location = "NULL"
	OR industry = "NULL"
	OR total_laid_off = "NULL"
	OR percentage_laid_off = "NULL"
	OR date_layoff = "NULL"
	OR stage = "NULL"
	OR country = "NULL"
	OR funds_raised_millions = "NULL";

SELECT *
FROM layoff_without_dupl
WHERE 
	company = ""
	OR location = ""
	OR industry = ""
	OR total_laid_off = ""
	OR percentage_laid_off = ""
	OR date_layoff = ""
	OR stage = ""
	OR country = ""
	OR funds_raised_millions = "";

PRAGMA table_info(layoff_without_dupl)

-- change all "" and "NULL" to NULL
-- we need to do this for every column
-- lets generate the code with python

--columns = """company
--location
--industry
--total_laid_off
--percentage_laid_off
--date_layoff
--stage
--country
--funds_raised_millions"""
--
--columns = columns.split("/n")
--
--for column in columns:
--    print(
--f"""UPDATE layoff_without_dupl
--SET {column} = NULL
--WHERE {column} = "" OR {column} = "NULL";
--
--""")
UPDATE layoff_without_dupl
SET company = NULL
WHERE company = "" OR company = "NULL";


UPDATE layoff_without_dupl
SET location = NULL
WHERE location = "" OR location = "NULL";


UPDATE layoff_without_dupl
SET industry = NULL
WHERE industry = "" OR industry = "NULL";


UPDATE layoff_without_dupl
SET total_laid_off = NULL
WHERE total_laid_off = "" OR total_laid_off = "NULL";


UPDATE layoff_without_dupl
SET percentage_laid_off = NULL
WHERE percentage_laid_off = "" OR percentage_laid_off = "NULL";


UPDATE layoff_without_dupl
SET date_layoff = NULL
WHERE date_layoff = "" OR date_layoff = "NULL";


UPDATE layoff_without_dupl
SET stage = NULL
WHERE stage = "" OR stage = "NULL";


UPDATE layoff_without_dupl
SET country = NULL
WHERE country = "" OR country = "NULL";


UPDATE layoff_without_dupl
SET funds_raised_millions = NULL
WHERE funds_raised_millions = "" OR funds_raised_millions = "NULL";

-- check if it worked
-- no matches for "NULL"
SELECT *
FROM layoff_without_dupl
WHERE 
	company = "NULL"
	OR location = "NULL"
	OR industry = "NULL"
	OR total_laid_off = "NULL"
	OR percentage_laid_off = "NULL"
	OR date_layoff = "NULL"
	OR stage = "NULL"
	OR country = "NULL"
	OR funds_raised_millions = "NULL";

-- no matches for ""
SELECT *
FROM layoff_without_dupl
WHERE 
	company = ""
	OR location = ""
	OR industry = ""
	OR total_laid_off = ""
	OR percentage_laid_off = ""
	OR date_layoff = ""
	OR stage = ""
	OR country = ""
	OR funds_raised_millions = "";

-- a lot of matches for IS NULL
SELECT *
FROM layoff_without_dupl
WHERE 
	company IS NULL
	OR location IS NULL
	OR industry IS NULL
	OR total_laid_off IS NULL
	OR percentage_laid_off IS NULL
	OR date_layoff IS NULL
	OR stage IS NULL
	OR country IS NULL
	OR funds_raised_millions IS NULL;

-- which NULL values can we populate?
-- company  - no NULLs
-- location - no NULLs
-- industry - can be populated if industry entrie for same company in another row available
-- total_laid_off - can not be populated
-- percentage_laid_off - can not be populated
-- date_layoff - can not be populated
-- stage - can be populated with "Unknown"
-- country - no NULLs

-- check every column if it can be populated
SELECT *
FROM layoff_without_dupl
WHERE country IS NULL;

-- delete all rows where total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoff_without_dupl
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE
FROM layoff_without_dupl
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- populate industry
SELECT *
FROM layoff_without_dupl
WHERE industry IS NULL;
-- Airbnb
-- Bally's Interactive
-- Carvana
-- Juul

SELECT *
FROM layoff_without_dupl
WHERE company = "Airbnb";

-- JOIN table with itself to fill empty industry entries
SELECT t1.industry, t2.industry 
FROM layoff_without_dupl t1 LEFT JOIN layoff_without_dupl t2
	ON t1.company = t2.company AND t1.location = t2.location
	WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;


-- update industry
UPDATE layoff_without_dupl
SET industry = (
	SELECT t2.industry
	FROM layoff_without_dupl t2
	WHERE layoff_without_dupl.company = t2.company 
		AND layoff_without_dupl.location = t2.location 
		AND t2.industry IS NOT NULL
)
WHERE industry IS NULL;

-- one company is missing because there is not industry mentioned
SELECT *
FROM layoff_without_dupl
WHERE industry IS NULL;

-- check table 
SELECT *
FROM layoff_without_dupl;

---------- date -----------

SELECT date_layoff 
FROM layoff_without_dupl;

SELECT date_layoff, REPLACE(date_layoff, "/", "-") AS new_date
FROM layoff_without_dupl
ORDER BY date_layoff DESC;

-- change date format
UPDATE layoff_without_dupl
SET date_layoff = REPLACE(date_layoff, "/", "-");

-- date is still a text type
SELECT *
FROM layoff_without_dupl;

-- date is in the wrong format
-- mm-dd-yyyy --> yyyy-mm-dd

-- mm-dd-yyyy
UPDATE layoff_without_dupl
SET date_layoff = (
	SUBSTR(date_layoff, 7, 4) || "-" || SUBSTR(date_layoff, 1, 2) || "-" || SUBSTR(date_layoff, 4, 2)
	)
WHERE date_layoff LIKE "__-__-____";

-- m-d-yyyy
UPDATE layoff_without_dupl
SET date_layoff = ( -- yyyy-mm-dd
	SUBSTR(date_layoff, 5, 4) || "-" || "0" || SUBSTR(date_layoff, 1, 1) || "-" || "0" || SUBSTR(date_layoff, 3, 1)
	)
WHERE date_layoff LIKE "_-_-____"; --m-d-yyyy

-- mm-d-yyyy
UPDATE layoff_without_dupl
SET date_layoff = ( -- yyyy-mm-dd
	SUBSTR(date_layoff, 6, 4) || "-" || SUBSTR(date_layoff, 1, 2) || "-" || "0" || SUBSTR(date_layoff, 4, 1)
	)
WHERE date_layoff LIKE "__-_-____"; --mm-d-yyyy

-- m-dd-yyyy
UPDATE layoff_without_dupl
SET date_layoff = ( -- yyyy-mm-dd
	SUBSTR(date_layoff, 6, 4) || "-" || "0" || SUBSTR(date_layoff, 1, 1) || "-" || SUBSTR(date_layoff, 3, 2)
	)
WHERE date_layoff LIKE "_-__-____"; --m-dd-yyyy

PRAGMA table_info(layoff_without_dupl);

SELECT*
FROM layoff_without_dupl;

-- table looks clean 
-- lets rename it
-- ALTER TABLE layoff_without_dupl RENAME TO layoff_cleaned;

CREATE TABLE layoff_cleaned AS
SELECT *
FROM layoff_without_dupl;



-------------- EDA - Exploratory Data Analysis ------------------
SELECT *
FROM layoff_cleaned;

-- Most layoffs by company
SELECT company, SUM(total_laid_off)
FROM layoff_cleaned
GROUP BY company
ORDER BY 2 DESC;

-- Biggest layoff at one time
SELECT company, total_laid_off 
FROM layoff_cleaned
ORDER BY 2 DESC;

-- Date range of data
SELECT MIN(date_layoff), MAX(date_layoff) 
FROM layoff_cleaned;

-- Most layoffs by industry
SELECT industry, SUM(total_laid_off)
FROM layoff_cleaned
GROUP BY industry
ORDER BY 2 DESC;

-- Most layoffs by country
SELECT country, SUM(total_laid_off)
FROM layoff_cleaned
GROUP BY country
ORDER BY 2 DESC;


-- Most layoffs by year
SELECT STRFTIME("%Y", date_layoff) AS year, SUM(total_laid_off)
FROM layoff_cleaned
GROUP BY STRFTIME("%Y", date_layoff)
ORDER BY 1 DESC;

-- rolling total of layoffs per month
SELECT 
	STRFTIME("%Y-%m", date_layoff) AS monthly,
	SUM(total_laid_off) AS total_off
FROM layoff_cleaned
WHERE monthly IS NOT NULL
GROUP BY monthly
ORDER BY 1 ASC;

WITH rolling_total AS 
	(
	SELECT 
		STRFTIME("%Y-%m", date_layoff) AS monthly,
		SUM(total_laid_off) AS total_off
	FROM layoff_cleaned
	WHERE monthly IS NOT NULL
	GROUP BY monthly
	ORDER BY 1 ASC
	)
SELECT 
	monthly,
	total_off,
	SUM(total_off) OVER(ORDER BY monthly) as rolling_sum
FROM rolling_total;

-- top five layoffs per company per year
SELECT
	company,
	STRFTIME("%Y", date_layoff) AS yearly,
	SUM(total_laid_off) AS laid_sum
FROM layoff_cleaned
WHERE yearly IS NOT NULL
GROUP BY company, yearly;

WITH cte AS (
SELECT
	company,
	STRFTIME("%Y", date_layoff) AS yearly,
	SUM(total_laid_off) AS laid_sum
FROM layoff_cleaned
WHERE yearly IS NOT NULL
GROUP BY company, yearly
), cte_2 AS
(
SELECT 
	*,
	DENSE_RANK() OVER (PARTITION BY yearly ORDER BY laid_sum DESC) AS ranking
FROM cte
WHERE laid_sum IS NOT NULL
ORDER BY yearly
)
SELECT *
FROM cte_2
WHERE ranking < 6;

