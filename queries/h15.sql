CREATE VIEW revenue[STREAM_ID] (supplier_no,
                                total_revenue) AS
SELECT l_suppkey,
       sum(l_extendedprice * (1 - l_discount))
FROM lineitem
WHERE l_shipdate >= '1996-01-01'
  AND l_shipdate < '1996-04-01'
GROUP BY l_suppkey
;


SELECT s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM supplier,
     revenue[STREAM_ID]
WHERE s_suppkey = supplier_no
  AND total_revenue =
    (SELECT max(total_revenue)
     FROM revenue[STREAM_ID])
ORDER BY s_suppkey
;


DROP VIEW revenue[STREAM_ID]
;
