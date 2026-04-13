reviews = LOAD '/input/hotel-review.csv' USING PigStorage(';') 
    AS (id:int, sentence:chararray, category:chararray, aspect:chararray, sentiment:chararray);

lowered = FOREACH reviews GENERATE id, LOWER(sentence) AS sentence, category, aspect, sentiment;
cleaned = FOREACH lowered GENERATE id,
    REPLACE(sentence, '[^\\p{L}\\s]', ' ') AS sentence,
    category, aspect, sentiment;
normalized = FOREACH cleaned GENERATE id,
    REPLACE(sentence, '\\s+', ' ') AS sentence,
    category, aspect, sentiment;

tokens = FOREACH normalized GENERATE id, TOKENIZE(sentence) AS words, category, aspect, sentiment;

stopwords = LOAD '/input/stopwords.txt' AS (sw:chararray);

flattened = FOREACH tokens GENERATE id, FLATTEN(words) AS word, category, aspect, sentiment;
joined = JOIN flattened BY word LEFT OUTER, stopwords BY sw;
filtered = FILTER joined BY sw IS NULL;

word_count = FOREACH (GROUP filtered BY (word, category, sentiment))
             GENERATE FLATTEN(group) AS (word, category, sentiment),
                      COUNT(filtered) AS num;

pos_words = FILTER word_count BY sentiment == 'positive';
neg_words = FILTER word_count BY sentiment == 'negative';

pos_sorted = ORDER pos_words BY num DESC;
neg_sorted = ORDER neg_words BY num DESC;

pos_grouped = GROUP pos_sorted BY category;
neg_grouped = GROUP neg_sorted BY category;

pos_top5 = FOREACH pos_grouped {
    ordered = ORDER pos_sorted BY num DESC;
    top5 = LIMIT ordered 5;
    GENERATE FLATTEN(top5);
};

neg_top5 = FOREACH neg_grouped {
    ordered = ORDER neg_sorted BY num DESC;
    top5 = LIMIT ordered 5;
    GENERATE FLATTEN(top5);
};

STORE pos_top5 INTO 'output_pos_top5' USING PigStorage('\t');
STORE neg_top5 INTO 'output_neg_top5' USING PigStorage('\t');

