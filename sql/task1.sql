SELECT DISTINCT ON (aet.name) 
aet.name AS aet,
DATE(dicom_stow_rs.created_at) AS first_scan
FROM aet
JOIN study ON study.calling_aet_id = aet.id
JOIN series ON series.study_id = study.id
JOIN dicom_stow_rs ON dicom_stow_rs.series_id = series.id
ORDER BY aet.name, dicom_stow_rs.created_at ASC;