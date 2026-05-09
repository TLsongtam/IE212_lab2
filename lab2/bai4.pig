data = LOAD 'file:///D:/VisualStudio/IE212/lab2/hotel-review.csv' 
       USING PigStorage(';') AS (id:int, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

stopword = LOAD 'file:///D:/VisualStudio/IE212/lab2/stopwords.txt' 
           USING PigStorage('\n') AS (word:chararray);

data = FOREACH data GENERATE id, LOWER(review) AS review, aspect, category, sentiment;

words = FOREACH data GENERATE 
    id,
    FLATTEN(TOKENIZE(review)) AS word,
    aspect, category, sentiment;

stopword_clean = FOREACH stopword GENERATE TRIM(word) AS word;

joined = JOIN words BY word LEFT OUTER, stopword_clean BY word;
filtered = FILTER joined BY stopword_clean::word IS NULL;

result_words = FOREACH filtered GENERATE 
    words::id AS id,
    words::word AS word,
    words::aspect AS aspect,
    words::category AS category,
    words::sentiment AS sentiment;

grouped = GROUP result_words BY (id, aspect, category, sentiment);
result = FOREACH grouped GENERATE
    FLATTEN(group) AS (id, aspect, category, sentiment),
    BagToString(result_words.word, ' ') AS clean_review;

-- Join lại với data gốc để giữ câu bị mất
all_grouped = GROUP data BY (id, aspect, category, sentiment);
all_data = FOREACH all_grouped GENERATE
    FLATTEN(group) AS (id, aspect, category, sentiment);

final = JOIN all_data BY (id, aspect, category, sentiment) LEFT OUTER, result BY (id, aspect, category, sentiment);

final_result = FOREACH final GENERATE
    all_data::id AS id,
    (result::clean_review IS NULL ? '' : result::clean_review) AS clean_review,
    all_data::aspect AS aspect,
    all_data::category AS category,
    all_data::sentiment AS sentiment;

final_sorted = ORDER final_result BY id ASC;

-- Bài 4: Top 5 từ theo sentiment cho từng category

-- Positive
pos_data = FILTER final_sorted BY sentiment == 'positive';
pos_words = FOREACH pos_data GENERATE category, FLATTEN(TOKENIZE(clean_review)) AS word;
pos_group_cat = GROUP pos_words BY (category, word);
pos_word_count = FOREACH pos_group_cat GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(pos_words) AS freq;
pos_group_all = GROUP pos_word_count BY category;
top5_pos = FOREACH pos_group_all GENERATE 
    group AS category, 
    TOP(5, 2, pos_word_count);


-- Negative
neg_data = FILTER final_sorted BY sentiment == 'negative';
neg_words = FOREACH neg_data GENERATE category, FLATTEN(TOKENIZE(clean_review)) AS word;
neg_group_cat = GROUP neg_words BY (category, word);
neg_word_count = FOREACH neg_group_cat GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(neg_words) AS freq;
neg_group_all = GROUP neg_word_count BY category;
top5_neg = FOREACH neg_group_all GENERATE 
    group AS category, 
    TOP(5, 2, neg_word_count);

final = UNION top5_neg, top5_pos;
STORE final INTO 'file:///D:/VisualStudio/IE212/lab2/output_bai4' USING PigStorage(';');