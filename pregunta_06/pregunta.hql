/* 

Pregunta
===========================================================================

Escriba una consulta que retorne unicamente la columna t0.c5 con sus 
elementos en mayuscula.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/

DROP TABLE IF EXISTS tbl0;
CREATE TABLE tbl0 (
    c1 INT,
    c2 STRING,
    c3 INT,
    c4 DATE,
    c5 ARRAY<CHAR(1)>, 
    c6 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data0.csv' INTO TABLE tbl0;

DROP TABLE IF EXISTS tbl1;
CREATE TABLE tbl1 (
    c1 INT,
    c2 INT,
    c3 STRING,
    c4 MAP<STRING, INT>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ':'
MAP KEYS TERMINATED BY '#'
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data1.csv' INTO TABLE tbl1;

/*
    >>> Escriba su respuesta a partir de este punto <<<
*/

-- CREATE TABLE docs AS SELECT UPPER(letter) FROM (SELECT c5 AS letter FROM tbl0)w;

-- INSERT OVERWRITE LOCAL DIRECTORY './output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
-- SELECT * FROM docs; 

CREATE TABLE datos1 AS SELECT transform(c5) USING '/bin/cat' AS (my_str) FROM tbl0;

CREATE TABLE datos1_ AS SELECT 
UPPER(regexp_replace(my_str, '\\[|\\]','')) AS final_string FROM datos1;

CREATE TABLE datos1_1 AS SELECT regexp_replace(final_string, '\\"|\\"','') AS final_ FROM datos1_;

INSERT OVERWRITE LOCAL DIRECTORY './output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT regexp_replace(final_, '\\,','\\:') FROM datos1_1;
