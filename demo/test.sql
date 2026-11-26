-- Test queries: 100 queries, 10 per template.
--   T1-T4:  the templates of train.sql, with literals not used in training (r10-r19, c10-c19).
--   T5-T10: templates that are not in train.sql at all.
-- Schema: demo/shop.sql. Used by the Quick Start (README 5) and demo/run.py.

-- T1
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r10' AND c.segment = 's10';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r11' AND c.segment = 's11';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r12' AND c.segment = 's12';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r13' AND c.segment = 's13';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r14' AND c.segment = 's14';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r15' AND c.segment = 's15';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r16' AND c.segment = 's16';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r17' AND c.segment = 's17';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r18' AND c.segment = 's18';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r19' AND c.segment = 's19';

-- T2
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c10' AND p.brand = 'b10';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c11' AND p.brand = 'b11';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c12' AND p.brand = 'b12';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c13' AND p.brand = 'b13';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c14' AND p.brand = 'b14';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c15' AND p.brand = 'b15';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c16' AND p.brand = 'b16';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c17' AND p.brand = 'b17';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c18' AND p.brand = 'b18';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c19' AND p.brand = 'b19';

-- T3
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r10' AND c.segment = 's10' AND p.category = 'c10' AND p.brand = 'b10';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r11' AND c.segment = 's11' AND p.category = 'c11' AND p.brand = 'b11';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r12' AND c.segment = 's12' AND p.category = 'c12' AND p.brand = 'b12';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r13' AND c.segment = 's13' AND p.category = 'c13' AND p.brand = 'b13';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r14' AND c.segment = 's14' AND p.category = 'c14' AND p.brand = 'b14';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r15' AND c.segment = 's15' AND p.category = 'c15' AND p.brand = 'b15';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r16' AND c.segment = 's16' AND p.category = 'c16' AND p.brand = 'b16';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r17' AND c.segment = 's17' AND p.category = 'c17' AND p.brand = 'b17';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r18' AND c.segment = 's18' AND p.category = 'c18' AND p.brand = 'b18';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r19' AND c.segment = 's19' AND p.category = 'c19' AND p.brand = 'b19';

-- T4
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r10' AND c.segment = 's10' AND c.age < 40;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r11' AND c.segment = 's11' AND c.age < 70;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r12' AND c.segment = 's12' AND c.age < 40;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r13' AND c.segment = 's13' AND c.age < 70;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r14' AND c.segment = 's14' AND c.age < 40;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r15' AND c.segment = 's15' AND c.age < 70;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r16' AND c.segment = 's16' AND c.age < 40;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r17' AND c.segment = 's17' AND c.age < 70;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r18' AND c.segment = 's18' AND c.age < 40;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r19' AND c.segment = 's19' AND c.age < 70;


-- T5: three tables with a date range
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r10' AND c.segment = 's10' AND o.odate >= date '2026-01-01' AND o.odate < date '2026-01-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r11' AND c.segment = 's11' AND o.odate >= date '2026-02-01' AND o.odate < date '2026-02-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r12' AND c.segment = 's12' AND o.odate >= date '2026-03-01' AND o.odate < date '2026-03-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r13' AND c.segment = 's13' AND o.odate >= date '2026-04-01' AND o.odate < date '2026-04-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r14' AND c.segment = 's14' AND o.odate >= date '2026-05-01' AND o.odate < date '2026-05-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r15' AND c.segment = 's15' AND o.odate >= date '2026-06-01' AND o.odate < date '2026-06-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r16' AND c.segment = 's16' AND o.odate >= date '2026-07-01' AND o.odate < date '2026-07-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r17' AND c.segment = 's17' AND o.odate >= date '2026-08-01' AND o.odate < date '2026-08-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r18' AND c.segment = 's18' AND o.odate >= date '2026-09-01' AND o.odate < date '2026-09-01' + 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r19' AND c.segment = 's19' AND o.odate >= date '2026-10-01' AND o.odate < date '2026-10-01' + 30;

-- T6: four tables with ranges on age and price
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r10' AND c.segment = 's10' AND c.age BETWEEN 10 AND 40 AND p.category = 'c10' AND p.brand = 'b10' AND p.price < 300;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r11' AND c.segment = 's11' AND c.age BETWEEN 15 AND 45 AND p.category = 'c11' AND p.brand = 'b11' AND p.price < 350;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r12' AND c.segment = 's12' AND c.age BETWEEN 20 AND 50 AND p.category = 'c12' AND p.brand = 'b12' AND p.price < 400;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r13' AND c.segment = 's13' AND c.age BETWEEN 25 AND 55 AND p.category = 'c13' AND p.brand = 'b13' AND p.price < 450;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r14' AND c.segment = 's14' AND c.age BETWEEN 30 AND 60 AND p.category = 'c14' AND p.brand = 'b14' AND p.price < 500;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r15' AND c.segment = 's15' AND c.age BETWEEN 35 AND 65 AND p.category = 'c15' AND p.brand = 'b15' AND p.price < 550;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r16' AND c.segment = 's16' AND c.age BETWEEN 40 AND 70 AND p.category = 'c16' AND p.brand = 'b16' AND p.price < 600;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r17' AND c.segment = 's17' AND c.age BETWEEN 45 AND 75 AND p.category = 'c17' AND p.brand = 'b17' AND p.price < 650;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r18' AND c.segment = 's18' AND c.age BETWEEN 50 AND 80 AND p.category = 'c18' AND p.brand = 'b18' AND p.price < 700;
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r19' AND c.segment = 's19' AND c.age BETWEEN 55 AND 85 AND p.category = 'c19' AND p.brand = 'b19' AND p.price < 750;

-- T7: four tables, grouped
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c20' AND p.brand = 'b20' AND o.odate >= date '2026-01-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c21' AND p.brand = 'b21' AND o.odate >= date '2026-02-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c22' AND p.brand = 'b22' AND o.odate >= date '2026-03-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c23' AND p.brand = 'b23' AND o.odate >= date '2026-04-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c24' AND p.brand = 'b24' AND o.odate >= date '2026-05-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c25' AND p.brand = 'b25' AND o.odate >= date '2026-06-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c26' AND p.brand = 'b26' AND o.odate >= date '2026-07-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c27' AND p.brand = 'b27' AND o.odate >= date '2026-08-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c28' AND p.brand = 'b28' AND o.odate >= date '2026-09-01' GROUP BY c.region;
SELECT c.region, count(*), sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c29' AND p.brand = 'b29' AND o.odate >= date '2026-10-01' GROUP BY c.region;

-- T8: IN lists on the correlated columns
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r10', 'r11') AND c.segment IN ('s10', 's11');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r11', 'r12') AND c.segment IN ('s11', 's12');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r12', 'r13') AND c.segment IN ('s12', 's13');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r13', 'r14') AND c.segment IN ('s13', 's14');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r14', 'r15') AND c.segment IN ('s14', 's15');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r15', 'r16') AND c.segment IN ('s15', 's16');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r16', 'r17') AND c.segment IN ('s16', 's17');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r17', 'r18') AND c.segment IN ('s17', 's18');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r18', 'r19') AND c.segment IN ('s18', 's19');
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region IN ('r19', 'r10') AND c.segment IN ('s19', 's10');

-- T9: EXISTS (semi-join) over three tables
SELECT count(*) FROM shop.customers c WHERE c.region = 'r10' AND c.segment = 's10' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c10' AND p.brand = 'b10');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r11' AND c.segment = 's11' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c11' AND p.brand = 'b11');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r12' AND c.segment = 's12' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c12' AND p.brand = 'b12');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r13' AND c.segment = 's13' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c13' AND p.brand = 'b13');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r14' AND c.segment = 's14' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c14' AND p.brand = 'b14');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r15' AND c.segment = 's15' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c15' AND p.brand = 'b15');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r16' AND c.segment = 's16' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c16' AND p.brand = 'b16');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r17' AND c.segment = 's17' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c17' AND p.brand = 'b17');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r18' AND c.segment = 's18' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c18' AND p.brand = 'b18');
SELECT count(*) FROM shop.customers c WHERE c.region = 'r19' AND c.segment = 's19' AND EXISTS (SELECT 1 FROM shop.orders o JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE o.customer_id = c.id AND p.category = 'c19' AND p.brand = 'b19');

-- T10: contradicting predicates on the correlated columns (no customer matches)
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r10' AND c.segment = 's11' AND p.category = 'c10' AND p.brand = 'b10';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r11' AND c.segment = 's12' AND p.category = 'c11' AND p.brand = 'b11';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r12' AND c.segment = 's13' AND p.category = 'c12' AND p.brand = 'b12';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r13' AND c.segment = 's14' AND p.category = 'c13' AND p.brand = 'b13';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r14' AND c.segment = 's15' AND p.category = 'c14' AND p.brand = 'b14';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r15' AND c.segment = 's16' AND p.category = 'c15' AND p.brand = 'b15';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r16' AND c.segment = 's17' AND p.category = 'c16' AND p.brand = 'b16';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r17' AND c.segment = 's18' AND p.category = 'c17' AND p.brand = 'b17';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r18' AND c.segment = 's19' AND p.category = 'c18' AND p.brand = 'b18';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r19' AND c.segment = 's10' AND p.category = 'c19' AND p.brand = 'b19';
