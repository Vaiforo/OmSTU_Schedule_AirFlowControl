SELECT
    lecturer_oid,
    lecturer,
    kind_of_work,
    countDistinct(lesson_oid) AS lessons_count,
    sum(duration) AS total_duration_units
FROM rasp_omgtu.schedule_lessons
WHERE lesson_date BETWEEN toDate('2026-03-13') AND toDate('2026-04-18')
GROUP BY
    lecturer_oid,
    lecturer,
    kind_of_work
ORDER BY
    lecturer,
    kind_of_work;