# Amazon Athena UDFs for Text Analytics using AWS Comprehend and AWS Translate

This connector extends Amazon Athena's capability by adding UDFs (via Lambda) for selected Amazon Comprehend and Amazon Translate APIs.

**To enable this Preview feature you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to use a UDF from that workgroup.**


#### UDF Performance
For language and sentiment detection, the comprehend UDFs take advantage of the Comprehend synchronous multi-document batch APIs to submit up to 25 records per request. 
The default API limit is 10 batch requests/sec (see [limits](https://docs.aws.amazon.com/comprehend/latest/dg/guidelines-and-limits.html) ), 
which results in a best case throughput of 250 rows/sec from the UDF (ignoring other overhead).
  
Sentiment detection also requires the source text language to be specified (e.g. `detect_sentiment()`). 
The batch sentiment API requires all inputs in a request to be in a single language. The UDF attempts to optimize throughput by batching consecutive records of the same 
language into a single request. The resulting batch sizes may be suboptimal if the input data is distributed across many languages with few consecutive records per language.

PII detection and Translation APIs do not have synchronous multi-document (batch) support; requests are limited to one input string at a time, and so throughput is 
limited by the default request throttle rate. (see [Comprehend limits](https://docs.aws.amazon.com/comprehend/latest/dg/guidelines-and-limits.html) 
and [Translate limits](https://docs.aws.amazon.com/translate/latest/dg/what-is-limits.html) ).

**NOTE:** Request Comprehend and/or Translate rate limit increases to increase the UDF throughput. 

## Translate

#### translate\_text(text_col VARCHAR, sourcelang VARCHAR, targetlang VARCHAR, terminologyname VARCHAR) RETURNS VARCHAR

Returns the translated string, in the target language specified. Source language can be explicitly specified, or use 'auto'
to detect source language automatically (the Translate service calls Comprehend behind the scenes to detect the source language when you use 'auto'). 
Specify a custom terminology name, or NULL if you aren't using custom terminologies.
```
USING FUNCTION translate_text(text_col VARCHAR, sourcelang VARCHAR, targetlang VARCHAR, customterminologyname VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT translate_text('It is a beautiful day in the neighborhood', 'auto', 'fr', NULL) as translated_text

translated_text
C'est une belle journée dans le quartier
```

## Detect Language

#### detect\_dominant\_language(text_col VARCHAR) RETURNS VARCHAR

Returns string value with dominant language code:
```
USING FUNCTION detect_dominant_language(text_col VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_dominant_language('il fait beau à Orlando') as language

language
fr
```
#### detect\_dominant\_language\_all(text_col VARCHAR) RETURNS VARCHAR

Returns the set of detected languages and scores as a JSON formatted string, which can be further analysed with Athena's `json_extract()` function.
```
USING FUNCTION detect_dominant_language_all(text_col VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_dominant_language_all('il fait beau à Orlando') as language_all

language_all
[{"languageCode":"fr","score":0.99807304}]
```

## Detect Sentiment

Input languages supported: en | es | fr | de | it | pt | ar | hi | ja | ko | zh | zh-TW (See [doc](https://docs.aws.amazon.com/comprehend/latest/dg/API_DetectSentiment.html#comprehend-DetectSentiment-request-LanguageCode) for latest)

#### detect\_sentiment(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR

Returns string value with dominant sentiment:

```
USING FUNCTION detect_sentiment(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_sentiment('Joe is very happy', 'en') as sentiment

sentiment
POSITIVE
```

#### detect\_sentiment\_all(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR

Returns the dominant sentiment and all sentiment scores as a JSON formatted string, which can be further analysed with Athena's `json_extract()` function.

```
USING FUNCTION detect_sentiment_all(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_sentiment_all('Joe is very happy', 'en') as sentiment_all

sentiment_all
{"sentiment":"POSITIVE","sentimentScore":{"positive":0.999519,"negative":7.407639E-5,"neutral":2.7478999E-4,"mixed":1.3210243E-4}}
```

## Detect and Redact Entities

Entity Types supported -- see [Entity types](https://docs.aws.amazon.com/comprehend/latest/dg/how-entities.html)
Input languages supported: en | es | fr | de | it | pt | ar | hi | ja | ko | zh | zh-TW (See [doc](https://docs.aws.amazon.com/comprehend/latest/dg/API_BatchDetectEntities.html#API_BatchDetectEntities_RequestSyntax) for latest)


#### detect\_entities(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR

Returns JSON string value with list of PII types and values:

```
USING FUNCTION detect_entities(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_entities('His name is Joe, he lives in Richmond VA, he bought an Amazon Echo Show on January 5th, and he loves it', 'en') as entities

entities
[["PERSON","Joe"],["LOCATION","Richmond VA"],["ORGANIZATION","Amazon"],["COMMERCIAL_ITEM","Echo Show"],["DATE","January 5th"]]
```

#### detect\_entities\_all(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR

Returns the detected entity types, scores, values, and offsets as a JSON formatted string, which can be further analysed with Athena's `json_extract()` function.

```
USING FUNCTION detect_entities_all(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_entities_all('His name is Joe, he lives in Richmond VA, he bought an Amazon Echo Show on January 5th, and he loves it', 'en') as entities_all

entities_all
[{"score":0.9956949,"type":"PERSON","text":"Joe","beginOffset":12,"endOffset":15},{"score":0.99672645,"type":"LOCATION","text":"Richmond VA","beginOffset":29,"endOffset":40},{"score":0.963684,"type":"ORGANIZATION","text":"Amazon","beginOffset":55,"endOffset":61},{"score":0.98822284,"type":"COMMERCIAL_ITEM","text":"Echo Show","beginOffset":62,"endOffset":71},{"score":0.998659,"type":"DATE","text":"January 5th","beginOffset":75,"endOffset":86}]
```

#### redact\_entities(text_col VARCHAR, lang VARCHAR, type VARCHAR) RETURNS VARCHAR

Redacts specified entity values from the input string.
Use the `types` argument to specify a list of [PII types](https://docs.aws.amazon.com/comprehend/latest/dg/API_PiiEntity.html#comprehend-Type-PiiEntity-Type) to be redacted.  

```
-- redact PERSON
USING FUNCTION redact_entities(text_col VARCHAR, lang VARCHAR, types VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT redact_entities('His name is Joe, he lives in Richmond VA, he bought an Amazon Echo Show on January 5th, and he loves it', 'en', 'PERSON') as entities_redacted

entities_redacted
His name is [PERSON], he lives in Richmond VA, he bought an Amazon Echo Show on January 5th, and he loves it

-- redact PERSON and DATE
USING FUNCTION redact_entities(text_col VARCHAR, lang VARCHAR, types VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT redact_entities('His name is Joe, he lives in Richmond VA, he bought an Amazon Echo Show on January 5th, and he loves it', 'en', 'PERSON, DATE') as entities_redacted

entities_redacted
His name is [PERSON], he lives in Richmond VA, he bought an Amazon Echo Show on [DATE], and he loves it

-- redact ALL Entity types
USING FUNCTION redact_entities(text_col VARCHAR, lang VARCHAR, types VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT redact_entities('His name is Joe, he lives in Richmond VA, he bought an Amazon Echo Show on January 5th, and he loves it', 'en', 'ALL') as entities_redacted

entities_redacted
His name is [PERSON], he lives in [LOCATION], he bought an [ORGANIZATION] [COMMERCIAL_ITEM] on [DATE], and he loves it
```


## Detect and Redact PII

PII Types supported -- see [PII types](https://docs.aws.amazon.com/comprehend/latest/dg/API_PiiEntity.html#comprehend-Type-PiiEntity-Type)
Input languages supported: 'en' (See [doc](https://docs.aws.amazon.com/comprehend/latest/dg/API_DetectPiiEntities.html#comprehend-DetectPiiEntities-request-LanguageCode) for latest)


#### detect\_pii\_entities(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR

Returns JSON string value with list of PII types and values:

```
USING FUNCTION detect_pii_entities(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_pii_entities('His name is Joe, his username is joe123 and he lives in Richmond VA', 'en') as pii

pii
[["NAME","Joe"],["USERNAME","joe123"],["ADDRESS","Richmond VA"]]
```

#### detect\_pii\_entities\_all(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR

Returns the detected PII types, scores, and offsets as a JSON formatted string, which can be further analysed with Athena's `json_extract()` function.

```
USING FUNCTION detect_pii_entities_all(text_col VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT detect_pii_entities_all('His name is Joe, his username is joe123 and he lives in Richmond VA', 'en') as pii_all

pii_all
[{"score":0.999894,"type":"NAME","beginOffset":12,"endOffset":15},{"score":0.99996245,"type":"USERNAME","beginOffset":33,"endOffset":39},{"score":0.9999982,"type":"ADDRESS","beginOffset":56,"endOffset":67}]
```

#### redact\_pii\_entities(text_col VARCHAR, lang VARCHAR, type VARCHAR) RETURNS VARCHAR

Redacts specified entity values from the input string.
Use the `types` argument to specify a list of [PII types](https://docs.aws.amazon.com/comprehend/latest/dg/API_PiiEntity.html#comprehend-Type-PiiEntity-Type) to be redacted.  

```
-- redact name
USING FUNCTION redact_pii_entities(text_col VARCHAR, lang VARCHAR, types VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT redact_pii_entities('His name is Joe, his username is joe123 and he lives in Richmond VA', 'en', 'NAME') as pii_redacted

pii_redacted
His name is [NAME], his username is joe123 and he lives in Richmond VA

-- redact NAME and ADDRESS
USING FUNCTION redact_pii_entities(text_col VARCHAR, lang VARCHAR, types VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT redact_pii_entities('His name is Joe, his username is joe123 and he lives in Richmond VA', 'en', 'NAME,ADDRESS') as pii_redacted

pii_redacted
His name is [NAME], his username is joe123 and he lives in [ADDRESS]

-- redact ALL PII types
USING FUNCTION redact_pii_entities(text_col VARCHAR, lang VARCHAR, types VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT redact_pii_entities('His name is Joe, his username is joe123 and he lives in Richmond VA', 'en', 'ALL') as pii_redacted

pii_redacted
His name is [NAME], his username is [USERNAME] and he lives in [ADDRESS]
```


## Use case examples

#### Analyze Amazon Product Reviews - sentiment by language

Dataset: See https://s3.amazonaws.com/amazon-reviews-pds/readme.html 

1. Create external table to access product reviews

```
CREATE EXTERNAL TABLE amazon_reviews_parquet(
  marketplace string, 
  customer_id string, 
  review_id string, 
  product_id string, 
  product_parent string, 
  product_title string, 
  star_rating int, 
  helpful_votes int, 
  total_votes int, 
  vine string, 
  verified_purchase string, 
  review_headline string, 
  review_body string, 
  review_date bigint, 
  year int)
PARTITIONED BY (product_category string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://amazon-reviews-pds/parquet/'
```

2. Create a new parquet table with detected languages and sentiment from 1996 product reviews (about 5000 reviews)
```
CREATE TABLE amazon_reviews_with_language_and_sentiment_1996
WITH (format='parquet') AS
USING FUNCTION detect_sentiment(col1 VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf')
SELECT *, detect_sentiment(review_headline, language) AS sentiment
FROM 
   (
   USING FUNCTION detect_dominant_language(col1 VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf')
   SELECT *, detect_dominant_language(review_headline) AS language
   FROM amazon_reviews_parquet
   )
WHERE language in ('ar', 'hi', 'ko', 'zh-TW', 'ja', 'zh', 'de', 'pt', 'en', 'it', 'fr', 'es')
AND year = 1996
```

*Notes:  
(1) Inner query detects the language in which the review was written.  
(2) Outer query detects the sentiment of the review using the detected language.  
(3) The languages are constrained to the set of languages supported by Comprehend's detectSentiment API.  
(4) Year is constrained to 1996 just to limit the scope to 5000 records, so query will run quickly (~1m)  


3. Explore results

```
SELECT sentiment, language, COUNT(*) as review_count 
FROM amazon_reviews_with_language_and_sentiment_1996
GROUP BY sentiment, language
ORDER BY sentiment, language 
```
Results show distribution of review by sentiment, for each language
```
sentiment	language	review_count
MIXED	en	179
NEGATIVE	en	392
NEGATIVE	fr	1
NEUTRAL	en	1159
NEUTRAL	fr	1
NEUTRAL	it	2
NEUTRAL	pt	2
POSITIVE	en	3301
POSITIVE	es	4
POSITIVE	it	1
```

4. Translate reviews to English from all source languages
```
CREATE TABLE amazon_reviews_normalised_to_english_1996
WITH (format='parquet') AS
USING FUNCTION translate_text(text_col VARCHAR, sourcelang VARCHAR, targetlang VARCHAR, terminologyname VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf')
SELECT *, translate_text(review_headline, language, 'en', NULL) as review_headline_en
FROM amazon_reviews_with_language_and_sentiment_1996
```
Take a look at the results:
```
SELECT lang, review_headline, review_headline_en
FROM amazon_reviews_normalised_to_english_1996
WHERE language <> 'en'
LIMIT 5

lang    review_headline                                               review_headline_en
pt	    A Alma Portuguesa	                                            The Portuguese Soul
es    	Magistral !!!	                                                Masterful!!!
fr	    On Oracle 7.1--Look for the 2nd Edition (ISBN 0782118402)    	On Oracle 7.1—Look for the 2nd Edition (ISBN 0782118402)
es	    In spanish (gran descripcion de la transformacion de la era)	In spanish (great description of the transformation of the era)
es	    ALUCINANTE.	                                                  MIND-BLOWING.


```

5. Look for PII

```
CREATE TABLE amazon_reviews_with_pii_1996
WITH (format='parquet') AS
USING FUNCTION detect_pii_entities(col1 VARCHAR, lang VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf')
SELECT *, detect_pii_entities(review_headline, language) AS pii
FROM amazon_reviews_with_language_and_sentiment_1996
WHERE language in ('en')

--- Example, look for NAMEs in the product reviews
SELECT review_headline, pii FROM amazon_reviews_with_pii_1996
WHERE pii LIKE '%ADDRESS%'

review_headline	                                              pii
Wistfully Abbey's best desert writing outside USA	            [["NAME","Abbey"],["ADDRESS","USA"]]
Highly recommended, richly atmospheric mystery set in Venice	[["ADDRESS","Venice"]]
An invaluable resource for keeping in touch across the USA.	  [["ADDRESS","USA"]]
Excellent detective trilogy set in 1940's Germany	            [["DATE_TIME","1940"],["ADDRESS","Germany"]]
&quot;Carrie&quot; meets &quot;Beverly Hills 90210&quot;	    [["NAME","Carrie"],["ADDRESS","Beverly Hills 90210"]]
Comprehensive, if dated, look at the town of Uniontown, MD.	  [["ADDRESS","Uniontown, MD"]]
Wouk meets Uris meets DeMille in Vietnam.	                    [["NAME","Wouk"],["NAME","Uris"],["NAME","DeMille"],["ADDRESS","Vietnam"]]
Good look at medical training in US	                          [["ADDRESS","US"]]
An excellent Story of Swedish Immigrants in Boston	          [["ADDRESS","Boston"]]
```
*Notes:  
(1) The languages are constrained to the set of languages supported by Comprehend's detectPiiEntities API (currently just en)  
(2) Runs in about 8 minutes (5000/rows) - slower than the others since there is no batch API for pii detection. Service rate limit rate will be required for larger record sets.  


#### Use JSON to extract fields from the Comprehend JSON full response

```
CREATE TABLE text_with_languages AS
USING FUNCTION detect_dominant_language_all(text_field VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'textanalytics-udf') 
SELECT text, detect_dominant_language_all(text) AS dominant_languages 
FROM
(
  SELECT * FROM (VALUES
    ('It is raining in Seattle, mais il fait beau à orlando'),
    ('It is raining in Seattle'),
    ('Esta lloviendo en seattle')
  ) AS t (text)
)


-- input text with full results from Comprehend
SELECT text, dominant_languages FROM text_with_languages ;

text	                                                  dominant_languages
It is raining in Seattle, mais il fait beau à orlando	  [{"languageCode":"fr","score":0.7244962},{"languageCode":"en","score":0.20402806}]
It is raining in Seattle	                              [{"languageCode":"en","score":0.9938028}]
Esta lloviendo en seattle	                              [{"languageCode":"es","score":0.98029345}]


-- input text, with only first detected language and score
SELECT text, json_extract(dominant_languages, '$[0]') AS language_and_score FROM text_with_languages

text	                                                  language_and_score
It is raining in Seattle, mais il fait beau à orlando   {"languageCode":"fr","score":0.7244962}
It is raining in Seattle	                              {"languageCode":"en","score":0.9938028}
Esta lloviendo en seattle	                              {"languageCode":"es","score":0.98029345}


-- input text, with dominant (1st) detected language and score selected as separate columns
SELECT 
    text, 
    json_extract(dominant_languages, '$[0].languageCode') AS language, 
    json_extract(dominant_languages, '$[0].score') AS score 
    FROM text_with_languages
    
text	                                                   language	score
It is raining in Seattle, mais il fait beau à orlando	  "fr"	    0.7244962
It is raining in Seattle	                              "en"	    0.9938028
Esta lloviendo en seattle	                              "es"	    0.98029345

```


  
  

### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of the connector Lambda. Then try the query examples above, or examples of your own, using the UDF.
  
  
Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-udfs-textanalytics dir, run `mvn clean install`.
3. From the athena-udfs-textanalytics dir, run  `./publish.sh <S3_BUCKET_NAME> ./athena-udfs-textanalytics us-east-1` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)
4. Deploy the lambda function from the serverless repo, or run `sam deploy --template-file packaged.yaml --stack-name TextAnalyticsUDFHandler --capabilities CAPABILITY_IAM`
4. Try using your UDF(s) in a query.

## License

This project is licensed under the Apache-2.0 License.