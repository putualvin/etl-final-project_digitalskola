DROP TABLE IF EXISTS digitalskola.district_case_number;

CREATE TABLE IF NOT EXISTS digitalskola.district_case_number (
              id SERIAL PRIMARY KEY,
              tanggal datetime,
              district_id VARCHAR(255),
              status_covid VARCHAR(255),
              total int);
INSERT INTO digitalskola.district_case_number(tanggal, district_id,status_covid, total)
SELECT
  tanggal,
  kode_kab,
  'closecontact_dikarantina' AS status,
  SUM(closecontact_dikarantina) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'closecontact_discarded' AS status,
  SUM(closecontact_discarded) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'closecontact_meninggal' AS status,
  SUM(closecontact_meninggal) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'confirmation_meninggal' AS status,
  SUM(confirmation_meninggal) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'confirmation_sembuh' AS status,
  SUM(confirmation_sembuh) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'probable_diisolasi' AS status,
  SUM(probable_diisolasi) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'probable_discarded' AS status,
  SUM(probable_discarded) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'probable_meninggal' AS status,
  SUM(probable_meninggal) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'suspect_diisolasi' AS status,
  SUM(suspect_diisolasi) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'suspect_discarded' AS status,
  SUM(suspect_discarded) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3
UNION ALL
SELECT
  tanggal,
  kode_kab,
  'suspect_meninggal' AS status,
  SUM(suspect_meninggal) AS sum_status
FROM digitalskola.covid_jabar
GROUP BY 1, 2, 3;

SELECT
    *
FROM digitalskola.district_case_number;