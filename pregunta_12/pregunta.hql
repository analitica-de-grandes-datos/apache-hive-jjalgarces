
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>, 
    c3 MAP<STRING, INT>
    )
    ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY '#'
        LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;


DROP TABLE IF EXISTS datos;

CREATE TABLE datos AS 
SELECT letra, key, value 
FROM (SELECT letra, c3 FROM t0 LATERAL VIEW EXPLODE (c2) t0 AS letra) datos2
LATERAL VIEW EXPLODE(c3) datos2;

INSERT OVERWRITE LOCAL DIRECTORY './output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT letra, key, COUNT(1)
FROM datos
GROUP BY letra, key;
