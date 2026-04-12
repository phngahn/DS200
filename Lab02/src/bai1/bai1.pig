reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, sentence:chararray, category:chararray, aspect:chararray, sentiment:chararray);

lowered = FOREACH reviews GENERATE id, LOWER(sentence) AS sentence, category, aspect, sentiment;

cleaned = FOREACH lowered GENERATE id,
    REPLACE(sentence, '[^\\p{L}\\s]', ' ') AS sentence,
    category, aspect, sentiment;

normalized = FOREACH cleaned GENERATE id,
    REPLACE(sentence, '\\s+', ' ') AS sentence,
    category, aspect, sentiment;

tokens = FOREACH normalized GENERATE id, TOKENIZE(sentence) AS words, category, aspect, sentiment;

stopwords = LOAD 'stopwords.txt' AS (sw:chararray);

flattened = FOREACH tokens GENERATE id, FLATTEN(words) AS word, category, aspect, sentiment;

joined = JOIN flattened BY word LEFT OUTER, stopwords BY sw;

filtered = FILTER joined BY sw IS NULL;

STORE filtered INTO 'output_final' USING PigStorage('\t');