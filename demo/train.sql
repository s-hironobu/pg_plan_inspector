-- Training queries (labels A: region/segment r0-r9, category/brand c0-c9).
-- Schema: demo/shop.sql. Used by the Quick Start (README 5) and demo/run.py.

-- T1
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r0' AND c.segment = 's0';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r1' AND c.segment = 's1';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r2' AND c.segment = 's2';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r3' AND c.segment = 's3';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r4' AND c.segment = 's4';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r5' AND c.segment = 's5';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r6' AND c.segment = 's6';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r7' AND c.segment = 's7';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r8' AND c.segment = 's8';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id WHERE c.region = 'r9' AND c.segment = 's9';

-- T2
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c0' AND p.brand = 'b0';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c1' AND p.brand = 'b1';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c2' AND p.brand = 'b2';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c3' AND p.brand = 'b3';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c4' AND p.brand = 'b4';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c5' AND p.brand = 'b5';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c6' AND p.brand = 'b6';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c7' AND p.brand = 'b7';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c8' AND p.brand = 'b8';
SELECT sum(l.qty) FROM shop.lineitems l JOIN shop.products p ON p.id = l.product_id WHERE p.category = 'c9' AND p.brand = 'b9';

-- T3
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r0' AND c.segment = 's0' AND p.category = 'c0' AND p.brand = 'b0';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r1' AND c.segment = 's1' AND p.category = 'c1' AND p.brand = 'b1';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r2' AND c.segment = 's2' AND p.category = 'c2' AND p.brand = 'b2';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r3' AND c.segment = 's3' AND p.category = 'c3' AND p.brand = 'b3';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r4' AND c.segment = 's4' AND p.category = 'c4' AND p.brand = 'b4';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r5' AND c.segment = 's5' AND p.category = 'c5' AND p.brand = 'b5';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r6' AND c.segment = 's6' AND p.category = 'c6' AND p.brand = 'b6';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r7' AND c.segment = 's7' AND p.category = 'c7' AND p.brand = 'b7';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r8' AND c.segment = 's8' AND p.category = 'c8' AND p.brand = 'b8';
SELECT count(*) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id JOIN shop.products p ON p.id = l.product_id WHERE c.region = 'r9' AND c.segment = 's9' AND p.category = 'c9' AND p.brand = 'b9';

-- T4
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r0' AND c.segment = 's0' AND c.age < 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r1' AND c.segment = 's1' AND c.age < 60;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r2' AND c.segment = 's2' AND c.age < 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r3' AND c.segment = 's3' AND c.age < 60;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r4' AND c.segment = 's4' AND c.age < 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r5' AND c.segment = 's5' AND c.age < 60;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r6' AND c.segment = 's6' AND c.age < 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r7' AND c.segment = 's7' AND c.age < 60;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r8' AND c.segment = 's8' AND c.age < 30;
SELECT sum(l.qty) FROM shop.customers c JOIN shop.orders o ON o.customer_id = c.id JOIN shop.lineitems l ON l.order_id = o.id WHERE c.region = 'r9' AND c.segment = 's9' AND c.age < 60;
