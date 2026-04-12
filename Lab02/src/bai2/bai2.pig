reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, sentence:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- Load dữ liệu đã xử lý từ Bài 1
processed = LOAD 'output_final/part-r-00000' USING PigStorage('\t') 
    AS (id:int, word:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- Thống kê tần số xuất hiện của các từ
word_freq = FOREACH (GROUP processed BY word)
            GENERATE group AS word, COUNT(processed) AS freq;

word_top = FILTER word_freq BY freq > 500;

-- Thống kê số bình luận theo từng phân loại (category)
cat_count = FOREACH (GROUP reviews BY category)
            GENERATE group AS category, COUNT(reviews) AS num_comments;

-- Thống kê số bình luận theo từng khía cạnh đánh giá (aspect)
aspect_count = FOREACH (GROUP reviews BY aspect)
               GENERATE group AS aspect, COUNT(reviews) AS num_comments;

-- Xuất kết quả
STORE word_top INTO 'output_word_top' USING PigStorage('\t');
STORE cat_count INTO 'output_category_count' USING PigStorage('\t');
STORE aspect_count INTO 'output_aspect_count' USING PigStorage('\t');
