-- Data Cleaning

select * 
from layoffs;

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1. Remove Duplicates

with duplicate_cte as
(
select *,
row_number() over (
partition by company,location,industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
#delete (not able to delete in mySQL, so create second staging table)
select *
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over (
partition by company,location,industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
where row_num > 1;


-- 2. Standardize Data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;


-- 3. Null or Blank Values


select distinct industry
from layoffs_staging2;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry = '';

#populate industry where missing
update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


-- 4. Remove Unecessary Data

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;


-- Exploratory Data Analysis

select *
from layoffs_staging2;

#date range
select min(`date`), max(`date`)
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

#companies with 100% laid off ordered by funds raised
select company,location,funds_raised_millions,country
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

#companies with most laid off
select company, sum(total_laid_off) total_laid_off
from layoffs_staging2
group by company
order by sum(total_laid_off) desc;

#industries with most laid off
select industry, sum(total_laid_off) total_laid_off
from layoffs_staging2
group by industry
order by sum(total_laid_off) desc;

#countries with most laid off
select country,sum(total_laid_off) total_laid_off
from layoffs_staging2
group by country
order by sum(total_laid_off) desc;

#years with most laid off
select year(`date`), sum(total_laid_off) total_laid_off
from layoffs_staging2
group by year(`date`)
order by 1 desc;

#companies stages with most laid off
select stage, sum(total_laid_off) total_laid_off
from layoffs_staging2
group by stage
order by total_laid_off desc;

#total laid off by month
select substring(`date`,1,7) as `Month`,sum(total_laid_off) 'Total Laid Off'
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by `month` asc;

#rolling total of laid off by month
with rolling_total as
(
select substring(`date`,1,7) as `Month`,sum(total_laid_off) total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by `month` asc
)
select `month`,
total_off,
sum(total_off) over(order by `month`) as rolling_total
from rolling_total;



select company, year(`date`), sum(total_laid_off) total_laid_off
from layoffs_staging2
group by company, year(`date`)
order by total_laid_off desc;

#top 5 companies with most laid off by year
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off) total_laid_off
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as
(select *, 
dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null)
select *
from company_year_rank
where ranking <= 5;