CREATE TABLE jobs_data
(
    id INT,
    job_id INT,
    job_title TEXT,
    salary_estimate TEXT,
    job_description TEXT,
    rating FLOAT,
    company_name TEXT,
    location TEXT,
    headquarters TEXT,
    size TEXT,
    founded INT,
    type_of_ownership TEXT,
    industry TEXT,
    sector TEXT,
    revenue TEXT,
    competitors TEXT,
    easy_apply TEXT
);

CREATE TABLE salary
(
    salary_id INT PRIMARY KEY,
    salary_min INT,
    salary_max INT
);
CREATE TABLE location
(
    location_id INT PRIMARY KEY,
    city TEXT,
    state TEXT
);
CREATE TABLE company
(
    company_id INT PRIMARY KEY,
    headquarters TEXT,
    type_of_ownership TEXT,
    industry TEXT,
    sector TEXT
);
CREATE TABLE fact_jobs
(
    fact_id INT PRIMARY KEY,
    job_title TEXT,
    rating FLOAT,
    founded INT,
    revenue TEXT,
    location_id INT,
    company_id INT,
    salary_id INT,
    FOREIGN KEY(location_id) REFERENCES location(location_id),
    FOREIGN KEY(company_id) REFERENCES company(company_id),
    FOREIGN KEY(salary_id) REFERENCES salary(salary_id)
)

--querys
-- numero de vacantes
SELECT COUNT(*)
FROM fact_jobs;

--numero de empresas distintas
SELECT COUNT(DISTINCT com.industry)
FROM fact_jobs fj
JOIN company com
ON fj.company_id= com.company_id;

--salario proemdio min y max
SELECT AVG(s.salary_min) as avg_min, AVG(s.salary_max) as avg_max
FROM salary s;

--Top 10 ciudades con más vacantes
SELECT  lo.state, COUNT(*) as num_jobs_per_state
FROM fact_jobs fj
JOIN location lo
ON fj.location_id = lo.location_id
GROUP BY  lo.state
ORDER BY num_jobs_per_state DESC
--LIMIT 10
;
--Salario promedio por estado
SELECT  lo.state, AVG(s.salary_min) as avg_min, AVG(s.salary_max) as avg_max
FROM fact_jobs fj
JOIN salary s
ON fj.salary_id = s.salary_id
JOIN location lo
ON fj.location_id = lo.location_id
GROUP BY lo.state
ORDER BY avg_max;

--Salario promedio por industria
SELECT  co.industry, AVG(s.salary_min) as avg_min, AVG(s.salary_max) as avg_max
FROM fact_jobs fj
JOIN salary s
ON fj.salary_id = s.salary_id
JOIN company co
ON fj.company_id = co.company_id
GROUP BY co.industry
ORDER BY avg_max;
