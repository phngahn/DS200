reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, sentence:chararray, category:chararray, aspect:chararray, sentiment:chararray);

aspect_sentiment = FOREACH (GROUP reviews BY (aspect, sentiment))
                   GENERATE FLATTEN(group) AS (aspect, sentiment),
                            COUNT(reviews) AS num_comments;

negative = FILTER aspect_sentiment BY sentiment == 'negative';
positive = FILTER aspect_sentiment BY sentiment == 'positive';

neg_sorted = ORDER negative BY num_comments DESC;
ranked_neg = RANK neg_sorted;
neg_top = FILTER ranked_neg BY rank_neg_sorted == 1;

pos_sorted = ORDER positive BY num_comments DESC;
ranked_pos = RANK pos_sorted;
pos_top = FILTER ranked_pos BY rank_pos_sorted == 1;

STORE neg_top INTO 'output_neg' USING PigStorage('\t');
STORE pos_top INTO 'output_pos' USING PigStorage('\t');
