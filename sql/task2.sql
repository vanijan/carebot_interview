SELECT DISTINCT ON (aet.name, prediction.prediction_string) 
aet.name AS aet_name,
prediction.prediction_string AS severity,
COUNT (*)
FROM aet
JOIN study ON study.calling_aet_id = aet.id
JOIN series ON series.study_id = study.id
JOIN dicom_stow_rs ON dicom_stow_rs.series_id = series.id
JOIN prediction ON prediction.dicom_stow_rs_id = dicom_stow_rs.id
GROUP BY severity, aet_name