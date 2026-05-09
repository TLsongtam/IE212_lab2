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

STORE final_sorted INTO 'file:///D:/VisualStudio/IE212/lab2/output' USING PigStorage(';');