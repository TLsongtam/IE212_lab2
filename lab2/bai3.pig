data = LOAD 'file:///D:/VisualStudio/IE212/lab2/hotel-review.csv'
       USING PigStorage(';') AS (id:int, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- Negative
neg = FILTER data BY sentiment == 'negative';
neg_group = GROUP neg BY aspect;
neg_count = FOREACH neg_group GENERATE group AS aspect, COUNT(neg) AS total, 'negative' AS sentiment;
neg_all = GROUP neg_count ALL;
top_neg = FOREACH neg_all GENERATE TOP(8, 1, neg_count);

-- Positive
pos = FILTER data BY sentiment == 'positive';
pos_group = GROUP pos BY aspect;
pos_count = FOREACH pos_group GENERATE group AS aspect, COUNT(pos) AS total, 'positive' AS sentiment;
pos_all = GROUP pos_count ALL;
top_pos = FOREACH pos_all GENERATE TOP(8, 1, pos_count);

-- Gộp lại
result = UNION top_neg, top_pos;
STORE result INTO 'file:///D:/VisualStudio/IE212/lab2/output_bai3' USING PigStorage(';');