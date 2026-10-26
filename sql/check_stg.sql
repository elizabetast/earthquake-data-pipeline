-- Завершённые загрузки, включая пустые (это допустимый результат API).
SELECT * FROM stg.batches ORDER BY request_started_at DESC LIMIT 10;
SELECT count(*) AS raw_rows, count(DISTINCT event_id) AS events FROM stg.earthquake_raw;
-- Ожидается 0 строк: технических дублей внутри batch нет.
SELECT batch_id,event_id,source_updated_at,payload_hash,count(*)
FROM stg.earthquake_raw GROUP BY 1,2,3,4 HAVING count(*) > 1;
-- Первые события: проверка контракта для роли 2.
SELECT id,event_id,source_updated_at,
       payload->'properties'->>'mag' AS magnitude,
       payload->'properties'->>'status' AS review_status,
       payload->'geometry'->'coordinates' AS coordinates
FROM stg.earthquake_raw ORDER BY id LIMIT 5;
