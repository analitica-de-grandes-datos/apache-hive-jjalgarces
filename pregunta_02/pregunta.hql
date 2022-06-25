/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Construya una consulta que ordene la tabla por letra y valor (3ra columna).

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (letter STRING, fec DATE, num INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="0");

LOAD DATA LOCAL INPATH 'data.tsv' OVERWRITE INTO TABLE docs;
INSERT OVERWRITE LOCAL DIRECTORY './output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT * FROM docs ORDER BY letter, num, YEAR(fec);
