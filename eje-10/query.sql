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
