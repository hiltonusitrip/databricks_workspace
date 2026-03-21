create or refresh streaming table books_silver;

CREATE FLOW books_flow
AS AUTO CDC INTO books_silver
FROM STREAM(books_raw)
KEYS (book_id)
SEQUENCE BY updated
COLUMNS * EXCEPT(updated)
STORED AS SCD TYPE 2;

CREATE OR REPLACE MATERIALIZED VIEW current_books
AS 
SELECT * FROM books_silver WHERE `__END_AT` IS NULL;

CREATE OR REPLACE STREAMING TABLE books_sales
AS
SELECT o.*, book.subtotal, c.*
FROM (
  SELECT *, EXPLODE(books) AS book
  FROM STREAM(orders_silver)
) o
INNER JOIN current_books c
ON book.book_id = c.book_id;


CREATE OR REPLACE STREAMING TABLE author_stats
AS
SELECT author,
window.start AS window_start,
window.end AS window_end,
count(order_id) AS total_orders,
sum(quantity) AS total_quantity,
avg(quantity) AS avg_quantity
FROM STREAM(books_sales)
GROUP BY author, window(order_timestamp, '5 minutes','5 minutes','2 minutes')
ORDER BY window.start;