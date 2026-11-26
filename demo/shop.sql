-- A small schema with the two classic traps for PostgreSQL's estimator:
--   * correlated predicates: region and segment are the same attribute in
--     disguise (both i % 20), category and brand likewise (both i % 50);
--   * skew: 40% of the orders belong to customers of region r0, and 40% of
--     the line items to products of category c0.
SELECT setseed(0.42);
DROP SCHEMA IF EXISTS shop CASCADE;
CREATE SCHEMA shop;

CREATE TABLE shop.customers AS
  SELECT i AS id, 'r' || (i % 20) AS region, 's' || (i % 20) AS segment,
         (random() * 100)::int AS age
  FROM generate_series(1, 50000) i;
CREATE TABLE shop.products AS
  SELECT i AS id, 'c' || (i % 50) AS category, 'b' || (i % 50) AS brand,
         (random() * 1000)::int AS price
  FROM generate_series(1, 5000) i;
CREATE TABLE shop.orders AS
  SELECT i AS id,
         CASE WHEN random() < 0.4 THEN 20 * (1 + floor(random() * 2499)::int)
              ELSE 1 + floor(random() * 50000)::int END AS customer_id,
         date '2026-01-01' + (random() * 300)::int AS odate
  FROM generate_series(1, 500000) i;
CREATE TABLE shop.lineitems AS
  SELECT i AS id, 1 + floor(random() * 500000)::int AS order_id,
         CASE WHEN random() < 0.4 THEN 50 * (1 + floor(random() * 99)::int)
              ELSE 1 + floor(random() * 5000)::int END AS product_id,
         1 + (random() * 9)::int AS qty
  FROM generate_series(1, 1500000) i;

ALTER TABLE shop.customers ADD PRIMARY KEY (id);
ALTER TABLE shop.products ADD PRIMARY KEY (id);
ALTER TABLE shop.orders ADD PRIMARY KEY (id);
ALTER TABLE shop.lineitems ADD PRIMARY KEY (id);
CREATE INDEX ON shop.orders (customer_id);
CREATE INDEX ON shop.lineitems (order_id);
CREATE INDEX ON shop.lineitems (product_id);
CREATE INDEX ON shop.customers (region);
CREATE INDEX ON shop.products (category);
VACUUM ANALYZE shop.customers, shop.products, shop.orders, shop.lineitems;
