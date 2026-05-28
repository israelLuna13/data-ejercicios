CREATE TABLE courses (
    id INT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    is_paid BOOLEAN NOT NULL,
    num_subscribers INT NOT NULL,
    avg_rating NUMERIC(3,2) NOT NULL,
    avg_rating_recent NUMERIC(3,2) NOT NULL,
    rating NUMERIC(3,2) NOT NULL,
    num_reviews INT NOT NULL,
    is_wishlisted BOOLEAN NOT NULL,
    num_published_lectures INT NOT NULL,
    num_published_practice_tests INT NOT NULL,
    created TIMESTAMP NOT NULL,
    published_time TIMESTAMP NOT NULL,
    discount_price_amount NUMERIC(10,2),
    discount_price_currency TEXT,
    discount_price_price_string TEXT,
    price_detail_amount NUMERIC(10,2) NOT NULL,
    price_detail_currency TEXT NOT NULL,
    price_detail_price_string TEXT NOT NULL
);

CREATE TABLE fact_udemy (
    id INT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    is_paid BOOLEAN NOT NULL,
    num_subscribers INT NOT NULL,
    avg_rating NUMERIC(3,2) NOT NULL,
    avg_rating_recent NUMERIC(3,2) NOT NULL,
    rating NUMERIC(3,2) NOT NULL,
    num_reviews INT NOT NULL,
    is_wishlisted BOOLEAN NOT NULL,
    num_published_lectures INT NOT NULL,
    num_published_practice_tests INT NOT NULL,
    created date,
    published_time date,
    discount_price_amount NUMERIC(10,2),
    price_detail_amount NUMERIC(10,2) NOT NULL
);

-- * Cuáles son los cursos con mejor rating
SELECT fu.title, fu.avg_rating_recent 
FROM fact_udemy fu
ORDER BY fu.avg_rating_recent DESC
--LIMIT 5
;

-- * Qué cursos tienen más suscriptores
SELECT fu.title, SUM(fu.num_subscribers) sum_subscribers    
FROM fact_udemy fu
GROUP BY fu.title
ORDER BY sum_subscribers DESC
--LIMIT 5
;
-- * Cuántos cursos son gratuitos vs pagados
SELECT  fu.is_paid, COUNT(fu.is_paid) as course
FROM fact_udemy fu
GROUP BY fu.is_paid
-- * Los cursos pagados tienen mejor rating que los gratuitos
SELECT  fu.is_paid, AVG(fu.avg_rating) as avg_rating
FROM fact_udemy fu
GROUP BY fu.is_paid;
-- * Cuáles son los cursos más caros
SELECT fu.title, fu.price_detail_amount
FROM fact_udemy fu
ORDER BY fu.price_detail_amount DESC
--LIMIT 5
;
-- * Qué cursos tienen más reviews
SELECT fu.title, fu.num_reviews
FROM fact_udemy fu
ORDER BY fu.num_reviews DESC
--LIMIT 5
-- * Qué cursos tienen más clases publicadas
SELECT fu.title, fu.num_published_practice_tests
FROM fact_udemy fu
ORDER BY fu.num_published_practice_tests DESC
--LIMIT 5