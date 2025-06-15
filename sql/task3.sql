WITH monthly_counts AS (
  SELECT
    aet.name,
    TO_CHAR(dicom_stow_rs.created_at, 'YYYY-MM') AS month,
    COUNT(*) AS entry_count
  FROM
    aet
    JOIN study ON study.calling_aet_id = aet.id
	JOIN series ON series.study_id = study.id
	JOIN dicom_stow_rs ON dicom_stow_rs.series_id = series.id
  GROUP BY
    aet.name,
    TO_CHAR(dicom_stow_rs.created_at, 'YYYY-MM')
)
SELECT
  name,
  month,
  entry_count,
  ROUND(AVG(entry_count) OVER (PARTITION BY name), 2) AS average_per_month
FROM
  monthly_counts
ORDER BY
  month, name;