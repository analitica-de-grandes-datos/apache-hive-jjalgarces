/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (letter STRING, fec DATE, num INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="0");

LOAD DATA LOCAL INPATH 'data.tsv' OVERWRITE INTO TABLE docs;

-- SELECT * FROM docs;

DROP TABLE IF EXISTS word_counts;
CREATE TABLE word_counts AS SELECT letter, COUNT(1) AS conteos
FROM docs GROUP BY letter ORDER BY letter; 

INSERT OVERWRITE LOCAL DIRECTORY './output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT * FROM word_counts;
