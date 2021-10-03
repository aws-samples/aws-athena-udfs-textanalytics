/*-
 * #%L
 * TextAnalyticsUDFHandler
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.amazonaws.athena.udf.textanalytics;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.services.translate.model.TranslateTextRequest;
import software.amazon.awssdk.services.translate.model.TranslateTextResponse;
import software.amazon.awssdk.services.translate.TranslateClient;
import software.amazon.awssdk.services.comprehend.model.BatchDetectDominantLanguageItemResult;
import software.amazon.awssdk.services.comprehend.model.BatchDetectDominantLanguageRequest;
import software.amazon.awssdk.services.comprehend.model.BatchDetectDominantLanguageResponse;
import software.amazon.awssdk.services.comprehend.model.BatchDetectEntitiesItemResult;
import software.amazon.awssdk.services.comprehend.model.BatchDetectEntitiesRequest;
import software.amazon.awssdk.services.comprehend.model.BatchDetectEntitiesResponse;
import software.amazon.awssdk.services.comprehend.model.BatchDetectKeyPhrasesItemResult;
import software.amazon.awssdk.services.comprehend.model.BatchDetectKeyPhrasesRequest;
import software.amazon.awssdk.services.comprehend.model.BatchDetectKeyPhrasesResponse;
import software.amazon.awssdk.services.comprehend.model.BatchDetectSentimentItemResult;
import software.amazon.awssdk.services.comprehend.model.BatchDetectSentimentRequest;
import software.amazon.awssdk.services.comprehend.model.BatchDetectSentimentResponse;
import software.amazon.awssdk.services.comprehend.model.BatchItemError;
import software.amazon.awssdk.services.comprehend.model.DetectPiiEntitiesRequest;
import software.amazon.awssdk.services.comprehend.model.DetectPiiEntitiesResponse;
import software.amazon.awssdk.services.comprehend.model.Entity;
import software.amazon.awssdk.services.comprehend.model.KeyPhrase;
import software.amazon.awssdk.services.comprehend.model.PiiEntity;
import software.amazon.awssdk.services.comprehend.model.SentimentScore;
import software.amazon.awssdk.services.comprehend.ComprehendClient;

import com.google.gson.Gson;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.text.BreakIterator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TextAnalyticsUDFHandler extends UserDefinedFunctionHandler
{
    private static final String SOURCE_TYPE = "athena_textanalytics_udf";
    public static int maxTextBytes = 5000;  //utf8 bytes
    public static int maxBatchSize = 25;
    
    private TranslateClient translateClient;
    private ComprehendClient comprehendClient;

    private ClientOverrideConfiguration createClientOverrideConfiguration()
    {
        // delays in milliseconds
        int retryBaseDelay = 500;
        int retryMaxBackoffTime = 600000;
        int maxRetries = 100;
        int timeout = 600000;
        RetryPolicy retryPolicy = RetryPolicy.defaultRetryPolicy().toBuilder()
            .numRetries(maxRetries)
            .backoffStrategy(EqualJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofMillis(retryBaseDelay))
                .maxBackoffTime(Duration.ofMillis(retryMaxBackoffTime))
                .build())
            .build();
        ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
            .apiCallTimeout(Duration.ofMillis(timeout))
            .apiCallAttemptTimeout(Duration.ofMillis(timeout))
            .retryPolicy(retryPolicy)
            .build();
        return clientOverrideConfiguration;
    }
    private ComprehendClient getComprehendClient() 
    {
        // create client first time on demand
        if (this.comprehendClient == null) {
            System.out.println("Creating Comprehend client connection");
            this.comprehendClient = ComprehendClient.builder()
                .overrideConfiguration(createClientOverrideConfiguration())
                .build();
            System.out.println("Created Comprehend client connection");
        }
        return this.comprehendClient;
    }
    private TranslateClient getTranslateClient() 
    {
        // create client first time on demand
        if (this.translateClient == null) {
            System.out.println("Creating Translate client connection");
            this.translateClient = TranslateClient.builder()
                .overrideConfiguration(createClientOverrideConfiguration())
                .build();
            System.out.println("Created Translate client connection");
        }
        return this.translateClient;
    }

    public TextAnalyticsUDFHandler()
    {
        super(SOURCE_TYPE);
    }

    /**
     * DETECT DOMINANT LANGUAGE
     * ========================
     **/
     
    /**
     * methods accepting and return JSON String paramater types, used by Athena UDF wrapper
     **/
    public String detect_dominant_language(String inputjson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        return toJSON(detect_dominant_language(input));
    }
    public String detect_dominant_language_all(String inputjson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        return toJSON(detect_dominant_language_all(input));
    } 

    /**
    * Given an array of input strings returns an array of language codes representing the detected dominant language of each input string
    * @param    input   an array of input strings
    * @return   an array of language code string values
    */
    public String[] detect_dominant_language(String[] input) throws Exception
    {
        return detect_dominant_language(input, false);
    }
    
    /**
    * Given an array of input strings returns an array of nested objects representing detected language code and confidence score for each input string
    * @param    input   an array of input strings
    * @return   an array of nested JSON objects with detect_dominant_language results for each input string
    */
    public String[] detect_dominant_language_all(String[] input) throws Exception
    {
        return detect_dominant_language(input, true);
    }   
    
    private String[] detect_dominant_language(String[] input, boolean fullResponse) throws Exception
    {
        // batch input records
        int rowCount = input.length;
        String[] result = new String[rowCount];
        int rowNum = 0;
        boolean splitLongText = false; // truncate, don't split long text fields.
        for (Object[] batch : getBatches(input, this.maxBatchSize, this.maxTextBytes, splitLongText)) {
            String[] textArray = (String[]) batch[0];
            String singleRowOrMultiRow = (String) batch[1];
            if (! singleRowOrMultiRow.equals("MULTI_ROW_BATCH")) {
                throw new RuntimeException("Error:  - Expected multirow batches only (truncate, not split): " + singleRowOrMultiRow);
            }
            System.out.println("DEBUG: Call comprehend BatchDetectDominantLanguage API - Split Batch => Records: " + textArray.length);
            // Call batchDetectDominantLanguage API
            BatchDetectDominantLanguageRequest batchDetectDominantLanguageRequest = BatchDetectDominantLanguageRequest.builder()
                    .textList(textArray)
                    .build();
            BatchDetectDominantLanguageResponse batchDetectDominantLanguageResponse = getComprehendClient().batchDetectDominantLanguage(batchDetectDominantLanguageRequest);
            // Throw exception if errorList is populated
            List<BatchItemError> batchItemError = batchDetectDominantLanguageResponse.errorList();
            if (! batchItemError.isEmpty()) {
                throw new RuntimeException("Error:  - ErrorList in batchDetectDominantLanguage result: " + batchItemError);
            }
            List<BatchDetectDominantLanguageItemResult> batchDetectDominantLanguageItemResult = batchDetectDominantLanguageResponse.resultList(); 
            for (int i = 0; i < batchDetectDominantLanguageItemResult.size(); i++) {
                if (fullResponse) {
                    // return JSON structure containing array of all detected languageCodes and scores
                    result[rowNum] = this.toJSON(batchDetectDominantLanguageItemResult.get(i).languages());
                }
                else {
                    // return simple string containing the languageCode of the first (most confident) language
                    result[rowNum] = batchDetectDominantLanguageItemResult.get(i).languages().get(0).languageCode();
                }
                rowNum++;
            }
        }
        return result;
    }

    /**
     * DETECT SENTIMENT
     * ================
     **/

    /**
     * methods accepting and return JSON String paramater types, used by Athena UDF wrapper
     **/
    public String detect_sentiment(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_sentiment(input, languageCodes));
    }
    public String detect_sentiment_all(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_sentiment_all(input, languageCodes));
    } 

    /**
    * Given an array of input strings returns an array of sentiment values representing the detected sentiment of each input string
    * @param    input    an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @return   an array of sentiment string values
    */
    public String[] detect_sentiment(String[] input, String[] languageCodes) throws Exception
    {
        return detect_sentiment(input, languageCodes, false);
    }   
    
    /**
    * Given an array of input strings returns an array of nested objects representing detected sentiment and confidence scores for each input string
    * @param    input    an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @return   an array of nested JSON objects with detect_sentiment results for each input string
    */
    public String[] detect_sentiment_all(String[] input, String[] languageCodes) throws Exception
    {
        return detect_sentiment(input, languageCodes, true);
    }   
    private String[] detect_sentiment(String[] input, String[] languageCodes, boolean fullResponse) throws Exception
    {
        // batch input records
        int rowCount = input.length;
        String[] result = new String[rowCount];
        int rowNum = 0;
        boolean splitLongText = false;  // truncate, don't split long text fields.
        for (Object[] batch : getBatches(input, languageCodes, this.maxBatchSize, this.maxTextBytes, splitLongText)) {
            String[] textArray = (String[]) batch[0];
            String singleRowOrMultiRow = (String) batch[1];
            String languageCode = (String) batch[2];
            if (! singleRowOrMultiRow.equals("MULTI_ROW_BATCH")) {
                throw new RuntimeException("Error:  - Expected multirow batches only (truncate, not split): " + singleRowOrMultiRow);
            }
            System.out.println("DEBUG: Call comprehend BatchDetectSentiment API - Batch => Language:" + languageCode + " Records: " + textArray.length);

            // Call batchDetectSentiment API
            BatchDetectSentimentRequest batchDetectSentimentRequest = BatchDetectSentimentRequest.builder()
                .textList(textArray)
                .languageCode(languageCode)
                .build();
            BatchDetectSentimentResponse batchDetectSentimentResponse = getComprehendClient().batchDetectSentiment(batchDetectSentimentRequest);
            // Throw exception if errorList is populated
            List<BatchItemError> batchItemError = batchDetectSentimentResponse.errorList();
            if (! batchItemError.isEmpty()) {
                throw new RuntimeException("Error:  - ErrorList in batchDetectSentiment result: " + batchItemError);
            }
            List<BatchDetectSentimentItemResult> batchDetectSentimentItemResult = batchDetectSentimentResponse.resultList(); 
            for (int i = 0; i < batchDetectSentimentItemResult.size(); i++) {
                if (fullResponse) {
                    // return JSON structure containing array of all sentiments and scores
                    String sentiment = batchDetectSentimentItemResult.get(i).sentiment().toString();
                    SentimentScore sentimentScore = batchDetectSentimentItemResult.get(i).sentimentScore();
                    result[rowNum] = "{\"sentiment\":" + toJSON(sentiment) + ",\"sentimentScore\":" + toJSON(sentimentScore) + "}";
                }
                else {
                    // return simple string containing the main sentiment
                    result[rowNum] = batchDetectSentimentItemResult.get(i).sentiment().toString();
                }
                rowNum++;
            }                
        }
        return result;
    }

    /**
     * DETECT ENTITIES
     * ================
     **/

    /**
     * methods accepting and return JSON String paramater types, used by Athena UDF wrapper
     **/
    public String detect_entities(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_entities(input, languageCodes));
    }
    public String detect_entities_all(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_entities_all(input, languageCodes));
    } 
    public String redact_entities(String inputjson, String languagejson, String redacttypesjson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        String[] redactTypesArray = fromJSON(redacttypesjson);
        return toJSON(redact_entities(input, languageCodes, redactTypesArray));        
    }

    /**
    * Given an array of input strings returns an array of nested arrays representing the detected entities (key/value pairs) in each input string
    * @param    input   an array of input strings
    * @param    languagejson an array of language codes corresponding to each input string
    * @return   an array of nested JSON arrays each representing a list of detected entities for each input string.
    */
    public String[] detect_entities(String[] input, String[] languageCodes) throws Exception
    {
        return detect_entities(input, languageCodes, new String[]{}, false);
    } 
    
    /**
    * Given an array of input strings returns an array of nested objects representing the detected entities and confidence scores for each input string
    * @param    input    an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @return   an array of nested JSON objects with detect_entities results for each input string
    */
    public String[] detect_entities_all(String[] input, String[] languageCodes) throws Exception
    {
        return detect_entities(input, languageCodes, new String[]{}, true);
    }  
    
    /**
    * Given an array of input strings with corresponding languages and entity types to redact, returns an array redacted strings
    * @param    input   an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @param    redacttypesjson an array of strings with comma-separated Entity Types to redact for each input string (or 'ALL' for all entity types)
    * @return   an array of strings with specified entity types redacted
    */
    public String[] redact_entities(String[] input, String[] languageCodes, String[] redactTypesArray) throws Exception
    {
        return detect_entities(input, languageCodes, redactTypesArray, false);
    }
    
    private String[] detect_entities(String[] input, String[] languageCodes, String[] redactTypesArray, boolean fullResponse) throws Exception
    {
        // batch input records
        int rowCount = input.length;
        String[] result = new String[rowCount];
        int rowNum = 0;
        boolean splitLongText = true; // split long text fields, don't truncate.
        for (Object[] batch : getBatches(input, languageCodes, this.maxBatchSize, this.maxTextBytes, splitLongText)) {
            String[] textArray = (String[]) batch[0];
            String singleRowOrMultiRow = (String) batch[1];
            String languageCode = (String) batch[2];
            System.out.println("DEBUG: Call comprehend BatchDetectEntities API - Batch => " + singleRowOrMultiRow + " Language:" + languageCode + " Records: " + textArray.length);
            if (singleRowOrMultiRow.equals("MULTI_ROW_BATCH")) {
                // batchArray represents multiple output rows, one element per output row
                int r1 = (redactTypesArray.length > 0) ? rowNum : 0;
                int r2 = (redactTypesArray.length > 0) ? rowNum + textArray.length : 0;
                String[] redactTypesArraySubset = Arrays.copyOfRange(redactTypesArray, r1, r2);
                String[] multiRowResults = MultiRowBatchDetectEntities(languageCode, textArray, redactTypesArraySubset, fullResponse);
                for (int i = 0; i < multiRowResults.length; i++) {
                    result[rowNum++] = multiRowResults[i];
                }
            }
            else {
                // batchArray represents single output row (text split)
                String redactTypes = (redactTypesArray.length > 0) ? redactTypesArray[rowNum] : "";
                String singleRowResults = TextSplitBatchDetectEntities(languageCode, textArray, redactTypes, fullResponse);
                result[rowNum++] = singleRowResults;
            }
        }
        return result;
    }

    private String[] MultiRowBatchDetectEntities(String languageCode, String[] batch, String[] redactTypes, boolean fullResponse) throws Exception
    {
        String[] result = new String[batch.length];
        // Call batchDetectEntities API
        BatchDetectEntitiesRequest batchDetectEntitiesRequest = BatchDetectEntitiesRequest.builder()
            .textList(batch)
            .languageCode(languageCode)
            .build();
        BatchDetectEntitiesResponse batchDetectEntitiesResponse = getComprehendClient().batchDetectEntities(batchDetectEntitiesRequest);
        // Throw exception if errorList is populated
        List<BatchItemError> batchItemError = batchDetectEntitiesResponse.errorList();
        if (! batchItemError.isEmpty()) {
            throw new RuntimeException("Error:  - ErrorList in batchDetectEntities result: " + batchItemError);
        }
        List<BatchDetectEntitiesItemResult> batchDetectEntitiesItemResult = batchDetectEntitiesResponse.resultList(); 
        if (batchDetectEntitiesItemResult.size() != batch.length) {
            throw new RuntimeException("Error:  - batch size and result item count do not match");
        }
        for (int i = 0; i < batchDetectEntitiesItemResult.size(); i++) {
            List<Entity> entities = batchDetectEntitiesItemResult.get(i).entities();
            if (fullResponse) {
                // return JSON structure containing all entity types, scores and offsets
                result[i] = this.toJSON(entities);
            }
            else {
                if (redactTypes.length == 0) {
                    // no redaction - return JSON string containing the entity types and extracted values
                    result[i] = getEntityTypesAndValues(entities);                      
                }
                else {
                    // redaction - return input string with specified entity types redacted
                    result[i] = redactEntityTypes(entities, batch[i], redactTypes[i]); 
                }
            }
        }
        return result;
    }
    private String TextSplitBatchDetectEntities(String languageCode, String[] input, String redactTypes, boolean fullResponse) throws Exception
    {
        String[] result = new String[input.length];
        int[] offset = new int[input.length];
        int rowNum = 0;
        // TODO: If batch length is more than max batch size, split into smaller batches and iterate
        for (Object[] batch : getBatches(input, this.maxBatchSize)) {
            String[] textArray = (String[]) batch[0];
            // Call batchDetectEntities API
            BatchDetectEntitiesRequest batchDetectEntitiesRequest = BatchDetectEntitiesRequest.builder()
                .textList(textArray)
                .languageCode(languageCode)
                .build();
            BatchDetectEntitiesResponse batchDetectEntitiesResponse = getComprehendClient().batchDetectEntities(batchDetectEntitiesRequest);
            // Throw exception if errorList is populated
            List<BatchItemError> batchItemError = batchDetectEntitiesResponse.errorList();
            if (! batchItemError.isEmpty()) {
                throw new RuntimeException("Error:  - ErrorList in batchDetectEntities result: " + batchItemError);
            }
            List<BatchDetectEntitiesItemResult> batchDetectEntitiesItemResult = batchDetectEntitiesResponse.resultList(); 
            if (batchDetectEntitiesItemResult.size() != textArray.length) {
                throw new RuntimeException("Error:  - array size " + textArray.length + " and result item count " + batchDetectEntitiesItemResult.size() + " do not match");
            }
            int cumOffset = 0;
            for (int i = 0; i < batchDetectEntitiesItemResult.size(); i++) {
                List<Entity> entities = batchDetectEntitiesItemResult.get(i).entities();
                if (fullResponse) {
                    // return JSON structure containing all entity types, scores and offsets
                    result[rowNum] = this.toJSON(entities);
                }
                else {
                    if (redactTypes.equals("")) {
                        // no redaction - return JSON string containing the entity types and extracted values
                        result[rowNum] = getEntityTypesAndValues(entities);                      
                    }
                    else {
                        // redaction - return input string with specified entity types redacted
                        result[rowNum] = redactEntityTypes(entities, textArray[i], redactTypes); 
                    }
                }
                offset[rowNum] = cumOffset;
                cumOffset += textArray[i].length();
                rowNum++;
            }
        }
        // merge results to single output row
        String mergedResult;
        if (fullResponse) {
            mergedResult = mergeEntitiesAll(result, offset);
        }
        else {
            if (redactTypes.equals("")) {
                mergedResult = mergeEntities(result);
            }
            else {
                mergedResult = mergeText(result);
            }
        }
        return mergedResult;
    }   
    private String getEntityTypesAndValues(List<Entity> entities) throws Exception
    {
        List<String[]> typesAndValues = new ArrayList<String[]>();
        for (Entity entity : entities) {
            String type = entity.type().toString();
            String value = entity.text();
            typesAndValues.add(new String[]{type, value});
        }
        String resultjson = toJSON(typesAndValues);
        return resultjson;
    }
    private String redactEntityTypes(List<Entity> entities, String text, String redactTypes) throws Exception
    {
        // redactTypes contains comma or space separated list of types, e.g. "NAME, ADDRESS"
        List<String> redactTypeList = Arrays.asList(redactTypes.split("[\\s,]+")); 
        String result = text;
        int deltaLength = 0;
        for (Entity entity : entities) {
            String type = entity.type().toString();
            if (redactTypes.contains(type) || redactTypes.contains("ALL")) {
                // this is a PII type we need to redact
                // Offset logic assumes piiEntity list is ordered by occurance in string
                int start = entity.beginOffset() + deltaLength;
                int end = entity.endOffset() + deltaLength;
                int length1 = result.length(); 
                result = new String(result.substring(0, start) + "[" + type + "]" + result.substring(end));
                deltaLength = deltaLength + (result.length() - length1);
            }
        }
        return result;
    }

    /**
     * DETECT / REDACT PII ENTITIES
     * =============================
     **/

    /**
     * methods accepting and return JSON String paramater types, used by Athena UDF wrapper
     **/
    public String detect_pii_entities(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_pii_entities(input, languageCodes));
    }
    public String detect_pii_entities_all(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_pii_entities_all(input, languageCodes));
    } 
    public String redact_pii_entities(String inputjson, String languagejson, String redacttypesjson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        String[] redactTypesArray = fromJSON(redacttypesjson);
        return toJSON(redact_pii_entities(input, languageCodes, redactTypesArray));        
    }

    /**
    * Given an array of input strings returns an array of nested JSON arrays representing the detected PII entities (key/value pairs) in each input string
    * @param    input   an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @return   an array of nested JSON arrays each representing a list of detected PII entities for each input string.
    */
    public String[] detect_pii_entities(String[] input, String[] languageCodes) throws Exception
    {
        return detect_pii_entities(input, languageCodes, new String[]{}, false);
    }    

    /**
    * Given an array of input strings returns an array of nested JSON objects representing the detected PII entities and confidence scores for each input string
    * @param    input    an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @return   an array of nested JSON objects with detect_pii_entities results for each input string
    */
    public String[] detect_pii_entities_all(String[] input, String[] languageCodes) throws Exception
    {
        return detect_pii_entities(input, languageCodes, new String[]{}, true);
    } 

    /**
    * Given an array of input strings with corresponding languages and PII entity types to redact, returns an array redacted strings
    * @param    input   an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @param    redactTypesArray an array of strings with comma-separated PII Entity Types to redact for each input string (or 'ALL' for all PII entity types)
    * @return   an array of strings with specified PII entity types redacted
    */
    public String[] redact_pii_entities(String[] input, String[] languageCodes, String[] redactTypesArray) throws Exception
    {
        return detect_pii_entities(input, languageCodes, redactTypesArray, false);
    }
    private String[] detect_pii_entities(String[] input, String[] languageCodes, String[] redactTypesArray, boolean fullResponse) throws Exception
    {
        // batch input records
        int rowCount = input.length;
        String[] result = new String[rowCount];
        int rowNum = 0;
        boolean splitLongText = true; // split long text fields, don't truncate.
        for (Object[] batch : getBatches(input, languageCodes, this.maxBatchSize, this.maxTextBytes, splitLongText)) {
            String[] textArray = (String[]) batch[0];
            String singleRowOrMultiRow = (String) batch[1];
            String languageCode = (String) batch[2];
            System.out.println("DEBUG: Call comprehend DetectPiiEntities API - Batch => Language:" + languageCode + " Records: " + textArray.length);
            if (singleRowOrMultiRow.equals("MULTI_ROW_BATCH")) {
                // batchArray represents multiple output rows, one element per output row
                int r1 = (redactTypesArray.length > 0) ? rowNum : 0;
                int r2 = (redactTypesArray.length > 0) ? rowNum + textArray.length : 0;
                String[] redactTypesArraySubset = Arrays.copyOfRange(redactTypesArray, r1, r2);
                String[] multiRowResults = MultiRowBatchDetectPiiEntities(languageCode, textArray, redactTypesArraySubset, fullResponse);
                for (int i = 0; i < multiRowResults.length; i++) {
                    result[rowNum++] = multiRowResults[i];
                }
            }
            else {
                // batchArray represents single output row (long text split)
                String redactTypes = (redactTypesArray.length > 0) ? redactTypesArray[rowNum] : "";
                String singleRowResults = TextSplitBatchDetectPiiEntities(languageCode, textArray, redactTypes, fullResponse);
                result[rowNum++] = singleRowResults;
            }
        }
        return result;
    }
    private String[] MultiRowBatchDetectPiiEntities(String languageCode, String[] batch, String[] redactTypes, boolean fullResponse) throws Exception
    {
        String[] result = new String[batch.length];
        // Call detectPiiEntities API in loop  (no multidocument batch API available)
        for (int i = 0; i < batch.length; i++) {
            DetectPiiEntitiesRequest detectPiiEntitiesRequest = DetectPiiEntitiesRequest.builder()
                .text(batch[i])
                .languageCode(languageCode)
                .build();
            DetectPiiEntitiesResponse detectPiiEntitiesResponse = getComprehendClient().detectPiiEntities(detectPiiEntitiesRequest);
            List<PiiEntity> piiEntities = detectPiiEntitiesResponse.entities(); 
            if (fullResponse) {
                // return JSON structure containing all entity types, scores and offsets
                result[i] = this.toJSON(piiEntities);
            }
            else {
                if (redactTypes.length == 0) {
                    // no redaction - return JSON string containing the entity types and extracted values
                    result[i] = getPiiEntityTypesAndValues(piiEntities, batch[i]);                      
                }
                else {
                    // redaction - return input string with specified PII types redacted
                    result[i] = redactPiiEntityTypes(piiEntities, batch[i], redactTypes[i]); 
                }
            }            
        }
        return result;
    }
    private String TextSplitBatchDetectPiiEntities(String languageCode, String[] batch, String redactTypes, boolean fullResponse) throws Exception
    {
        String[] result = new String[batch.length];
        int[] offset = new int[batch.length];
        // Call detectPiiEntities API in loop  (no multidocument batch API available)
        int cumOffset = 0;
        for (int i = 0; i < batch.length; i++) {
            DetectPiiEntitiesRequest detectPiiEntitiesRequest = DetectPiiEntitiesRequest.builder()
                .text(batch[i])
                .languageCode(languageCode)
                .build();
            DetectPiiEntitiesResponse detectPiiEntitiesResponse = getComprehendClient().detectPiiEntities(detectPiiEntitiesRequest);
            List<PiiEntity> piiEntities = detectPiiEntitiesResponse.entities();
            if (fullResponse) {
                // return JSON structure containing all entity types, scores and offsets
                result[i] = this.toJSON(piiEntities);
            }
            else {
                if (redactTypes.equals("")) {
                    // no redaction - return JSON string containing the entity types and extracted values
                    result[i] = getPiiEntityTypesAndValues(piiEntities, batch[i]);                      
                }
                else {
                    // redaction - return input string with specified PII types redacted
                    result[i] = redactPiiEntityTypes(piiEntities, batch[i], redactTypes); 
                }
            } 
            offset[i] = cumOffset;
            cumOffset += batch[i].length();
        }
        // merge results to single output row
        String mergedResult;
        if (fullResponse) {
            mergedResult = mergeEntitiesAll(result, offset);
        }
        else {
            if (redactTypes.equals("")) {
                mergedResult = mergeEntities(result);
            }
            else {
                mergedResult = mergeText(result);
            }
        }
        return mergedResult;
    }   
    private String getPiiEntityTypesAndValues(List<PiiEntity> piiEntities, String text) throws Exception
    {
        List<String[]> typesAndValues = new ArrayList<String[]>();
        for (PiiEntity piiEntity : piiEntities) {
            String type = piiEntity.type().toString();
            String value = text.substring(piiEntity.beginOffset(), piiEntity.endOffset());
            typesAndValues.add(new String[]{type, value});
        }
        String resultjson = toJSON(typesAndValues);
        return resultjson;
    }
    private String redactPiiEntityTypes(List<PiiEntity> piiEntities, String text, String redactTypes) throws Exception
    {
        // redactTypes contains comma or space separated list of types, e.g. "NAME, ADDRESS"
        List<String> redactTypeList = Arrays.asList(redactTypes.split("[\\s,]+")); 
        String result = text;
        int deltaLength = 0;
        for (PiiEntity piiEntity : piiEntities) {
            String type = piiEntity.type().toString();
            if (redactTypes.contains(type) || redactTypes.contains("ALL")) {
                // this is a PII type we need to redact
                // Offset logic assumes piiEntity list is ordered by occurance in string
                int start = piiEntity.beginOffset() + deltaLength;
                int end = piiEntity.endOffset() + deltaLength;
                int length1 = result.length(); 
                result = new String(result.substring(0, start) + "[" + type + "]" + result.substring(end));
                deltaLength = deltaLength + (result.length() - length1);
            }
        }
        return result;
    }

    /**
     * DETECT KEY PHRASES
     * ==================
     **/

    /**
     * methods accepting and return JSON String paramater types, used by Athena UDF wrapper
     **/
    public String detect_key_phrases(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_key_phrases(input, languageCodes));
    }
    public String detect_key_phrases_all(String inputjson, String languagejson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        return toJSON(detect_key_phrases_all(input, languageCodes));
    } 

    /**
    * Given an array of input strings returns an array of nested arrays representing the detected key phrases (key/value pairs) in each input string
    * @param    input   an array of input strings
    * @param    languagejson an array of language codes corresponding to each input string
    * @return   an array of nested JSON arrays each representing a list of detected key phrases for each input string.
    */
    public String[] detect_key_phrases(String[] input, String[] languageCodes) throws Exception
    {
        return detect_key_phrases(input, languageCodes, false);
    } 
    
    /**
    * Given an array of input strings returns an array of nested objects representing the detected entities and confidence scores for each input string
    * @param    input    an array of input strings
    * @param    languageCodes an array of language codes corresponding to each input string
    * @return   an array of nested JSON objects with detect_key_phrases results for each input string
    */
    public String[] detect_key_phrases_all(String[] input, String[] languageCodes) throws Exception
    {
        return detect_key_phrases(input, languageCodes, true);
    }  

    private String[] detect_key_phrases(String[] input, String[] languageCodes, boolean fullResponse) throws Exception
    {
        // batch input records
        int rowCount = input.length;
        String[] result = new String[rowCount];
        int rowNum = 0;
        boolean splitLongText = true; // split long text fields, don't truncate.
        for (Object[] batch : getBatches(input, languageCodes, this.maxBatchSize, this.maxTextBytes, splitLongText)) {
            String[] textArray = (String[]) batch[0];
            String singleRowOrMultiRow = (String) batch[1];
            String languageCode = (String) batch[2];
            System.out.println("DEBUG: Call comprehend BatchDetectKeyPhrases API - Batch => " + singleRowOrMultiRow + " Language:" + languageCode + " Records: " + textArray.length);
            if (singleRowOrMultiRow.equals("MULTI_ROW_BATCH")) {
                // batchArray represents multiple output rows, one element per output row
                String[] multiRowResults = MultiRowDetectKeyPhrases(languageCode, textArray, fullResponse);
                for (int i = 0; i < multiRowResults.length; i++) {
                    result[rowNum++] = multiRowResults[i];
                }
            }
            else {
                // batchArray represents single output row (text split)
                String singleRowResults = TextSplitBatchDetectKeyPhrases(languageCode, textArray, fullResponse);
                result[rowNum++] = singleRowResults;
            }
        }
        return result;
    }

    private String[] MultiRowDetectKeyPhrases(String languageCode, String[] batch, boolean fullResponse) throws Exception
    {
        String[] result = new String[batch.length];
        // Call batchDetectKeyPhrases API
        BatchDetectKeyPhrasesRequest batchDetectKeyPhrasesRequest = BatchDetectKeyPhrasesRequest.builder()
            .textList(batch)
            .languageCode(languageCode)
            .build();
        BatchDetectKeyPhrasesResponse batchDetectKeyPhrasesResponse = getComprehendClient().batchDetectKeyPhrases(batchDetectKeyPhrasesRequest);
        // Throw exception if errorList is populated
        List<BatchItemError> batchItemError = batchDetectKeyPhrasesResponse.errorList();
        if (! batchItemError.isEmpty()) {
            throw new RuntimeException("Error:  - ErrorList in batchDetectKeyPhrases result: " + batchItemError);
        }
        List<BatchDetectKeyPhrasesItemResult> batchDetectKeyPhrasesItemResult = batchDetectKeyPhrasesResponse.resultList(); 
        if (batchDetectKeyPhrasesItemResult.size() != batch.length) {
            throw new RuntimeException("Error:  - batch size and result item count do not match");
        }
        for (int i = 0; i < batchDetectKeyPhrasesItemResult.size(); i++) {
            List<KeyPhrase> keyPhrases = batchDetectKeyPhrasesItemResult.get(i).keyPhrases();
            if (fullResponse) {
                // return JSON structure containing all key phrases, scores and offsets
                result[i] = this.toJSON(keyPhrases);
            }
            else {
                result[i] = getKeyPhraseValues(keyPhrases);                      
            }
        }
        return result;
    }
    private String TextSplitBatchDetectKeyPhrases(String languageCode, String[] input, boolean fullResponse) throws Exception
    {
        String[] result = new String[input.length];
        int[] offset = new int[input.length];
        int rowNum = 0;
        // TODO: If batch length is more than max batch size, split into smaller batches and iterate
        for (Object[] batch : getBatches(input, this.maxBatchSize)) {
            String[] textArray = (String[]) batch[0];
            // Call batchDetectEntities API
            BatchDetectKeyPhrasesRequest batchDetectKeyPhrasesRequest = BatchDetectKeyPhrasesRequest.builder()
                .textList(textArray)
                .languageCode(languageCode)
                .build();
            BatchDetectKeyPhrasesResponse batchDetectKeyPhrasesResponse = getComprehendClient().batchDetectKeyPhrases(batchDetectKeyPhrasesRequest);
            // Throw exception if errorList is populated
            List<BatchItemError> batchItemError = batchDetectKeyPhrasesResponse.errorList();
            if (! batchItemError.isEmpty()) {
                throw new RuntimeException("Error:  - ErrorList in batchDetectKeyPhrases result: " + batchItemError);
            }
            List<BatchDetectKeyPhrasesItemResult> batchDetectKeyPhrasesItemResult = batchDetectKeyPhrasesResponse.resultList(); 
            if (batchDetectKeyPhrasesItemResult.size() != textArray.length) {
                throw new RuntimeException("Error:  - array size " + textArray.length + " and result item count " + batchDetectKeyPhrasesItemResult.size() + " do not match");
            }
            int cumOffset = 0;
            for (int i = 0; i < batchDetectKeyPhrasesItemResult.size(); i++) {
                List<KeyPhrase> keyPhrases = batchDetectKeyPhrasesItemResult.get(i).keyPhrases();
                if (fullResponse) {
                    // return JSON structure containing all entity types, scores and offsets
                    result[rowNum] = this.toJSON(keyPhrases);
                }
                else {
                    result[rowNum] = getKeyPhraseValues(keyPhrases);                      
                }
                offset[rowNum] = cumOffset;
                cumOffset += textArray[i].length();
                rowNum++;
            }
        }
        // merge results to single output row
        String mergedResult;
        if (fullResponse) {
            mergedResult = mergeEntitiesAll(result, offset);
        }
        else {
            mergedResult = mergeEntities(result);
        }
        return mergedResult;
    }   
    private String getKeyPhraseValues(List<KeyPhrase> keyPhrases) throws Exception
    {
        List<String> values = new ArrayList<String>();
        for (KeyPhrase keyPhrase : keyPhrases) {
            values.add(keyPhrase.text());
        }
        String resultjson = toJSON(values);
        return resultjson;
    }

    /**
     * TRANSLATE TEXT
     */

    /**
     * method accepting and return JSON String paramater types, used by Athena UDF wrapper
     **/
    public String translate_text(String inputjson, String sourcelanguagejson, String targetlanguagejson, String terminologynamesjson) throws Exception
    {
        String[] input = fromJSON(inputjson);
        String[] sourceLanguageCodes = fromJSON(sourcelanguagejson);
        String[] targetLanguageCodes = fromJSON(targetlanguagejson);
        String[] terminologyNames = fromJSON(terminologynamesjson);
        return toJSON(translate_text(input, sourceLanguageCodes, targetLanguageCodes, terminologyNames));
    }

    /**
    * Given an array of input strings, source language, target language, and optional terminology names, returns an array of translated strings
    * @param    input    an array of input strings
    * @param    sourceLanguageCodes an array of source language codes corresponding to each input string (source lang can be 'auto' if source language is unknown)
    * @param    targetLanguageCodes an array of target language codes
    * @param    terminologyNames an array of custom terminology (CT) names in Amazon Translate (or 'NULL' if CT is not to be applied)
    * @return   an array of translated string values
    */
    public String[] translate_text(String[] input, String[] sourceLanguageCodes, String[] targetLanguageCodes, String[] terminologyNames) throws Exception
    {
        // batch input records
        int rowCount = input.length;
        String[] result = new String[rowCount];
        int rowNum = 0;
        boolean splitLongText = true; // split long text fields, don't truncate.
        for (Object[] batch : getBatches(input, this.maxBatchSize, this.maxTextBytes, splitLongText)) {
            String[] textArray = (String[]) batch[0];
            String singleRowOrMultiRow = (String) batch[1];
            if (singleRowOrMultiRow.equals("MULTI_ROW_BATCH")) {
                // batchArray represents multiple output rows, one element per output row
                System.out.println("DEBUG: Call MultiRowBatchTranslateText Translatetext API - Batch => Records: " + textArray.length);
                String[] sourceLanguageCodesSubset = Arrays.copyOfRange(sourceLanguageCodes, rowNum, rowNum + textArray.length);
                String[] targetLanguageCodesSubset = Arrays.copyOfRange(targetLanguageCodes, rowNum, rowNum + textArray.length);
                String[] terminologyNamesSubset = Arrays.copyOfRange(terminologyNames, rowNum, rowNum + textArray.length);
                String[] multiRowResults = MultiRowBatchTranslateText(textArray, sourceLanguageCodesSubset, targetLanguageCodesSubset, terminologyNamesSubset);
                for (int i = 0; i < multiRowResults.length; i++) {
                    result[rowNum++] = multiRowResults[i];
                }
            }
            else {
                // batchArray represents single output row (long text split)
                System.out.println("DEBUG: Call TextSplitBatchTranslateText Translatetext API - Batch => Records: " + textArray.length);
                String sourceLanguageCode = sourceLanguageCodes[rowNum];
                String targetLanguageCode = targetLanguageCodes[rowNum];
                String terminologyName = terminologyNames[rowNum];
                String singleRowResults = TextSplitBatchTranslateText(textArray, sourceLanguageCode, targetLanguageCode, terminologyName);
                result[rowNum++] = singleRowResults;
            }
        }
        return result;
    }
    private String[] MultiRowBatchTranslateText(String[] batch, String[] sourceLanguageCodesSubset, String[] targetLanguageCodesSubset, String[] terminologyNamesSubset) throws Exception
    {
        String[] result = new String[batch.length];
        // Call translateText API in loop  (no multidocument batch API available)
        for (int i = 0; i < batch.length; i++) {
            TranslateTextRequest translateTextRequest = TranslateTextRequest.builder()
                .sourceLanguageCode(sourceLanguageCodesSubset[i])
                .targetLanguageCode(targetLanguageCodesSubset[i])
                .text(batch[i])
                .build();
            if (! terminologyNamesSubset[i].equals("null")) {
                translateTextRequest = translateTextRequest.toBuilder().terminologyNames(terminologyNamesSubset[i]).build();
            }
            try {
                TranslateTextResponse translateTextResponse = getTranslateClient().translateText(translateTextRequest);
                String translatedText = translateTextResponse.translatedText();  
                result[i] = translatedText;
            } 
            catch (Exception e) {
                System.out.println("ERROR: Translate API Exception.\nInput String size: " + getUtf8StringLength(batch[i]) + " bytes. String:\n" + batch[i]);
                System.out.println("EXCEPTION:\n" + e);
                // return input text untranslated
                result[i] = batch[i];
            }
        }
        return result;
    }
    private String TextSplitBatchTranslateText(String[] batch, String sourceLanguageCode, String targetLanguageCode, String terminologyName) throws Exception
    {
        String[] result = new String[batch.length];
        // Call translateText API in loop  (no multidocument Translate API available)
        for (int i = 0; i < batch.length; i++) {
            TranslateTextRequest translateTextRequest = TranslateTextRequest.builder()
                .sourceLanguageCode(sourceLanguageCode)
                .targetLanguageCode(targetLanguageCode)
                .text(batch[i])
                .build();
            if (! terminologyName.equals("null")) {
                translateTextRequest = translateTextRequest.toBuilder().terminologyNames(terminologyName).build();
            }
            try {
                TranslateTextResponse translateTextResponse = getTranslateClient().translateText(translateTextRequest);
                String translatedText = translateTextResponse.translatedText();  
                result[i] = translatedText;
            } 
            catch (Exception e) {
                System.out.println("ERROR: Translate API Exception.\nInput String size: " + getUtf8StringLength(batch[i]) + " bytes. String:\n" + batch[i]);
                System.out.println("EXCEPTION:\n" + e);
                // return input text untranslated
                result[i] = batch[i];
            }
        }
        // merge results to single output row
        String mergedResult = mergeText(result);
        return mergedResult;
    }       
    
    /**
     * PRIVATE HELPER METHODS
     * 
     */
     
    // merges multiple results from detectEntities or detectPiiEntities into a single string
    private static String mergeEntities(String[] arrayOfJson) throws Exception
    {
        JSONArray resultArray = new JSONArray();
        for (int i = 0; i < arrayOfJson.length; i++) {
            JSONArray entities = new JSONArray(arrayOfJson[i]);
            resultArray.putAll(entities);
        }
        return resultArray.toString();
    }
    // merges multiple results from detectEntities_all or detectPiiEntities_all into a single string
    // apply offsets to the beginOffset and endOffset members of each detected entity
    private static String mergeEntitiesAll(String[] arrayOfJson, int[] offset) throws Exception
    {
        JSONArray resultArray = new JSONArray();
        for (int i = 0; i < arrayOfJson.length; i++) {
            JSONArray entities = new JSONArray(arrayOfJson[i]);
            JSONArray entityResultWithOffset = applyOffset(entities, offset[i]);
            resultArray.putAll(entities);
        }
        return resultArray.toString();   
    }
    // merges multiple results from redactEntities or redactPiiEntities_all into a single string
    private static String mergeText(String[] arrayOfStrings) throws Exception
    {
        return (String.join("", arrayOfStrings));
    }
    // apply offset to the values of beginOffset and endOffset in each result, so that they match the original long input text
    private static JSONArray applyOffset(JSONArray entities, int offset) throws Exception
    {
        System.out.println("Entities DEBUG: " + entities);
        int size = entities.length();
        for (int i = 0; i < size; i++) {
            JSONObject entity = entities.getJSONObject(i);
            int beginOffset = entity.getInt("beginOffset");
            int endOffset = entity.getInt("endOffset");
            entity.put("beginOffset", beginOffset + offset);
            entity.put("endOffset", endOffset + offset);
        }
        return entities;
    }
    
    // splits input array into batches no larger than multiDocBatchSize
    private List<Object[]> getBatches(String[] input, int multiRowBatchSize)
        throws Exception
    {
        List<Object[]> batches = new ArrayList<Object[]>();
        int start = 0;
        int c = 0;
        for (int i = 0; i < input.length; i++) {
            if (c++ >= multiRowBatchSize) {
                // add a batch, and reset c
                batches.add(new Object[] {Arrays.copyOfRange(input, start, i), "MULTI_ROW_BATCH"});
                start = i;
                c = 1;
            }
        }
        // last split
        if (start < input.length) {
            batches.add(new Object[] {Arrays.copyOfRange(input, start, input.length), "MULTI_ROW_BATCH"});
        }
        return batches;        
    }
    // as above, but also checks utf-8 byte size for input and can return batch for single input record containing splits
    private List<Object[]> getBatches(String[] input, int multiRowBatchSize, int maxTextBytes, boolean splitLongText)
        throws Exception
    {
        List<Object[]> batches = new ArrayList<Object[]>();
        int start = 0;
        int c = 0;
        for (int i = 0; i < input.length; i++) {
            if (c++ >= multiRowBatchSize) {
                // add a batch (not including current row), and reset c
                batches.add(new Object[] {Arrays.copyOfRange(input, start, i), "MULTI_ROW_BATCH"});
                start = i;
                c = 1;
            }
            int textLength = getUtf8StringLength(input[i]);
            boolean tooLong = (textLength >= maxTextBytes) ? true : false;
            if (tooLong && !splitLongText) {
                // truncate this row
                System.out.println("Truncating long text field (" + textLength + " bytes) to " + maxTextBytes + " bytes");
                input[i] = truncateUtf8(input[i], maxTextBytes);
            }
            if (tooLong && splitLongText) {
                // close off current multi-record batch before making new single record batch
                if (start < i) {
                    batches.add(new Object[] {Arrays.copyOfRange(input, start, i), "MULTI_ROW_BATCH"});
                }
                // split this row and add the text splits as a new *TEXT_SPLIT_BATCH* batch
                String[] textSplit = splitLongText(input[i], maxTextBytes);
                System.out.println("Split long text field (" + textLength + " bytes) into " + textSplit.length + " segments of under " + maxTextBytes + " bytes");
                batches.add(new Object[] {textSplit, "TEXT_SPLIT_BATCH"});
                // increment counters for next row / next batch
                start = i + 1;
                c = 1;                 
            }            
        }
        // last multi-record split
        if (start < input.length) {
            batches.add(new Object[] {Arrays.copyOfRange(input, start, input.length), "MULTI_ROW_BATCH"});
        }
        return batches;         
    }

    // as above, but also splits input array into batches representing one language only
    private List<Object[]> getBatches(String[] input, String[] languageCodes, int multiRowBatchSize, int maxTextBytes, boolean splitLongText)
        throws Exception
    {
        List<Object[]> batches = new ArrayList<Object[]>();
        String languageCode = languageCodes[0];
        int start = 0;
        int c = 0;
        for (int i = 0; i < input.length; i++) {
            if (c++ >= multiRowBatchSize || ! languageCode.equals(languageCodes[i])) {
                // add a batch (not including current row), and reset c
                batches.add(new Object[] {Arrays.copyOfRange(input, start, i), "MULTI_ROW_BATCH", languageCode});
                languageCode = languageCodes[i];
                start = i;
                c = 1;
            }
            int textLength = getUtf8StringLength(input[i]);
            boolean tooLong = (textLength > maxTextBytes) ? true : false;
            if (tooLong && !splitLongText) {
                // truncate this row
                System.out.println("Truncating long text field (" + textLength + " bytes) to " + maxTextBytes + " bytes");
                input[i] = truncateUtf8(input[i], maxTextBytes);
            }
            if (tooLong && splitLongText) {
                // close off current multi-record batch before making new single record batch
                if (start < i) {
                    batches.add(new Object[] {Arrays.copyOfRange(input, start, i), "MULTI_ROW_BATCH", languageCode});
                }
                // split this row and add the text splits as a new *TEXT_SPLIT_BATCH* batch
                String[] textSplit = splitLongText(input[i], maxTextBytes);
                System.out.println("Split long text field (" + textLength + " bytes) into " + textSplit.length + " segments of under " + maxTextBytes + " bytes");
                batches.add(new Object[] {textSplit, "TEXT_SPLIT_BATCH", languageCode});
                // increment counters for next row / next batch
                start = i + 1;
                c = 1;
                if (i < input.length) {
                    languageCode = languageCodes[i];
                }
            } 
        }
        // last multi-record split
        if (start < input.length) {
            batches.add(new Object[] {Arrays.copyOfRange(input, start, input.length), "MULTI_ROW_BATCH", languageCode});
        }
        return batches;          
    }

    private static int getUtf8StringLength(String string) throws Exception
    {
        final byte[] utf8Bytes = string.getBytes("UTF-8");
        return (utf8Bytes.length);        
    }

    /**
     * truncates a string to fit designated number of UTF-8 bytes
     * Needed to comply with Comprehend's input string limit of 5000 UTF-8 bytes
     * NOTE - not the same as String.length(), which counts (multi-byte) chars
     */
    private static String truncateUtf8(String string, int maxBytes) throws Exception
    {
        CharsetEncoder enc = StandardCharsets.UTF_8.newEncoder();
        ByteBuffer bb = ByteBuffer.allocate(maxBytes); // note the limit
        CharBuffer cb = CharBuffer.wrap(string);
        CoderResult r = enc.encode(cb, bb, true);
        if (r.isOverflow()) {
            string = cb.flip().toString();
        }
        return string;
    }

    private static String[] splitLongText(String longText, int maxTextBytes) throws Exception
    {
        String[] sentences = splitStringBySentence(longText);
        // recombine sentences up to maxTextBytes
        List<String> splitBatches = new ArrayList<String>();
        int bytesCnt = 0;
        int start = 0;
        for (int i = 0; i < sentences.length; i++) {
            int sentenceLength = getUtf8StringLength(sentences[i]);
            if (sentenceLength >= maxTextBytes) {
                System.out.println("DATA WARNING: sentence size (" + sentenceLength + " bytes) is larger than max (" + maxTextBytes + " bytes). Unsplittable.");
                System.out.println("Problematic sentence: " + sentences[i]);
                // TODO - Truncate, or drop?
            }
            bytesCnt += sentenceLength;
            if (bytesCnt >= maxTextBytes) {
                // join sentences prior to this one, and add to splitBatches. Reset counters.
                String splitBatch = String.join("", Arrays.copyOfRange(sentences, start, i));
                int splitBatchLength = getUtf8StringLength(splitBatch);
                if (splitBatchLength == 0 || splitBatchLength > maxTextBytes) {
                    System.out.println("DEBUG: Split size is " + splitBatchLength + " bytes - Skipping.");
                } 
                else {
                    System.out.println("DEBUG: Split size (" + splitBatchLength + " bytes)");
                    splitBatches.add(splitBatch);
                }
                start = i;
                bytesCnt = getUtf8StringLength(sentences[i]);
            }
        }
        // last split
        if (start < sentences.length) {
            String splitBatch = String.join("", Arrays.copyOfRange(sentences, start, sentences.length));
            int splitBatchLength = getUtf8StringLength(splitBatch);
            if (splitBatchLength == 0 || splitBatchLength > maxTextBytes) {
                System.out.println("DEBUG: Split size is " + splitBatchLength + " bytes - Skipping.");
            } 
            else {
                System.out.println("DEBUG: Split size (" + splitBatchLength + " bytes)");
                splitBatches.add(splitBatch);
            }
        }
        String[] splitArray = (String[]) splitBatches.toArray(new String[0]);
        return splitArray;
    }

    private static String[] splitStringBySentence(String longText) 
    {
        BreakIterator boundary = BreakIterator.getSentenceInstance();
        boundary.setText(longText);
        List<String> sentencesList = new ArrayList<String>();
        int start = boundary.first();
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            sentencesList.add(longText.substring(start, end));
        }
        String[] sentenceArray = (String[]) sentencesList.toArray(new String[0]);
        return sentenceArray;
    }
 
    private static String toJSON(Object obj) 
    {
        Gson gson = new Gson();
        return gson.toJson(obj);
    }

    private static String[] fromJSON(String json) 
    {
        Gson gson = new Gson();
        return gson.fromJson(json, String[].class);
    }

    /**
     * Processes a group by rows. This method takes in a block of data (containing multiple rows), process them and
     * returns multiple rows of the output column in a block.
     * <p>
     * In the super class UDF methods are invoked row-by-row in a for loop. 
     * This override method greatly improves throughput by batching records into 
     * fewer calls using the Comprehend batch APIs.
     *
     * @param allocator arrow memory allocator
     * @param udfMethod the extracted java method matching the User-Defined-Function defined in Athena.
     * @param inputRecords input data in Arrow format
     * @param outputSchema output data schema in Arrow format
     * @return output data in Arrow format
     */
    @Override
    protected Block processRows(BlockAllocator allocator, Method udfMethod, Block inputRecords, Schema outputSchema)
            throws Exception
    {
        int rowCount = inputRecords.getRowCount();
        System.out.println("DEBUG: inputRecords rowCount = " + rowCount);
        int fieldCount = inputRecords.getFieldReaders().size();
        System.out.println("DEBUG: inputRecords fieldCount = " + fieldCount);

        String[][] input = new String[fieldCount][rowCount];
        for (int fieldNum = 0; fieldNum < fieldCount; ++fieldNum) {
            for (int rowNum = 0; rowNum < rowCount; ++rowNum) {
                input[fieldNum][rowNum] = this.getStringValue(inputRecords, fieldNum, rowNum);
                //System.out.println("FIELD " + fieldNum + " /ROW " + rowNum + " VALUE: " + input[fieldNum][rowNum]);
            }
        }
        // input and output arrays serialised to JSON strings, to match the method signature declared in the UDF.
        String[] inputjson = new String[fieldCount];
        for (int fieldNum = 0; fieldNum < fieldCount; ++fieldNum) {
            inputjson[fieldNum] = toJSON(input[fieldNum]);  
        }
        // now call the udf with the right number of arguments, per fieldCount
        String resultjson;
        switch (fieldCount) {
            case 1: resultjson = (String) udfMethod.invoke(this, inputjson[0]);
                    break;
            case 2: resultjson = (String) udfMethod.invoke(this, inputjson[0], inputjson[1]);
                    break;
            case 3: resultjson = (String) udfMethod.invoke(this, inputjson[0], inputjson[1], inputjson[2]);
                    break;
            case 4: resultjson = (String) udfMethod.invoke(this, inputjson[0], inputjson[1], inputjson[2], inputjson[3]);
                    break;
            default: throw new RuntimeException("Error: invalid argument count - " + fieldCount);
        }
        String[] result = fromJSON(resultjson);
        Field outputField = outputSchema.getFields().get(0);
        Block outputRecords = allocator.createBlock(outputSchema);
        outputRecords.setRowCount(rowCount);
        for (int rowNum = 0; rowNum < rowCount; ++rowNum) {
            outputRecords.setValue(outputField.getName(), rowNum, result[rowNum]);
        }        
        return outputRecords;
    }
    
    /**
     * Used to convert a specific field from row in the provided Block to a String value. 
     * Code adapted from BlockUtils.rowToString.
     *
     * @param block The Block to read the row from.
     * @param field The field number to read.
     * @param row The row number to read.
     * @return The String representation of the requested row.
     */
    private static String getStringValue(Block block, int field, int row)
    {
        if (row > block.getRowCount()) {
            throw new IllegalArgumentException(row + " exceeds available rows " + block.getRowCount());
        }
        StringBuilder sb = new StringBuilder();
        FieldReader fieldReader = block.getFieldReaders().get(field);
        fieldReader.setPosition(row);
        sb.append(BlockUtils.fieldToString(fieldReader));            
        return sb.toString();
    }

    /**
     * Testing
     **/
     
    static void runSplitBySentenceTests(TextAnalyticsUDFHandler textAnalyticsUDFHandler) throws Exception
    {
        String[] resultArr;
        String result;
        System.out.println("Test splitting text by sentence");
        String longText = new String("My name is Mr. P. A. Jeremiah Smith Jr., and I live at 1234 Summer Dr., Anytown, USA. This sentence has 10.5 words, and some abbreviations, e.g. this one. Also: punctuation in quotes, like this, \"Way to go Joe!\", she said.");
        System.out.println("Original text: " + longText);
        resultArr = textAnalyticsUDFHandler.redact_pii_entities(makeArray(longText, 1), makeArray("en", 1), makeArray("ALL", 1));
        System.out.println("Original - PII Redacted: " + String.join("", resultArr)); 
        resultArr = textAnalyticsUDFHandler.redact_entities(makeArray(longText, 1), makeArray("en", 1), makeArray("ALL", 1));
        System.out.println("Original - Entities Redacted: " + String.join("", resultArr)); 
        String[] sentenceArray = splitStringBySentence(longText);
        System.out.println("Split sentences: \n" + String.join("\n", sentenceArray)); 
        int cnt = sentenceArray.length;
        resultArr = textAnalyticsUDFHandler.redact_pii_entities(sentenceArray, makeArray("en", cnt), makeArray("ALL", cnt));
        result = String.join("", resultArr);
        System.out.println("Text Split, PII Redacted and combined: " + result); 
        resultArr = textAnalyticsUDFHandler.redact_entities(sentenceArray, makeArray("en", cnt), makeArray("ALL", cnt));
        result = String.join("", resultArr);
        System.out.println("Text Split, Entities Redacted and combined: " + result); 
    }
    
    static void runStringLengthTests() throws Exception
    {
        String longText = "je déteste ça et je m'appelle Bob";
        System.out.println("Original text: " + longText + "\nOriginal length bytes: " + getUtf8StringLength(longText) + " Original length chars: " + longText.length());
        String truncated = truncateUtf8(longText, 20);
        System.out.println("Truncated text: " + truncated + "\nNew length bytes: " + getUtf8StringLength(truncated) + " New length chars: " + truncated.length());
    }
    
    static void runSplitLongTextTest() throws Exception
    {
        int maxTextBytes = 70;
        String longText = "My name is Jeremiah. I live in Anytown, USA. I am 35 years old. I am 5'7\" tall. I love cars, and dogs. My SSN is 123-45-6789. My cell is (707)555-1234.";
        System.out.println("Test slitting long text blocks to under " + maxTextBytes + " UTF-8 bytes");
        String[] splits = splitLongText(longText, maxTextBytes);
        System.out.println("Split of long text: \n" + String.join("\n", splits));
    }
    
    
    
    static void runMergeEntitiesTests() throws Exception
    {
        String[] arrayOfJsonObjects = new String[] {
            "[{\"type\":\"NAME\",\"beginOffset\":1,\"endOffset\":5},{\"type\":\"ADDRESS\",\"beginOffset\":5,\"endOffset\":10}]",
            "[{\"type\":\"NAME\",\"beginOffset\":1,\"endOffset\":5},{\"type\":\"ADDRESS\",\"beginOffset\":5,\"endOffset\":10}]",
            "[{\"type\":\"NAME\",\"beginOffset\":1,\"endOffset\":5}]"
        };
        int[] offset = new int[] {0, 10, 20};
        System.out.println(mergeEntitiesAll(arrayOfJsonObjects, offset));
        String[] arrayOfJsonArrays = new String[] {
            "[[\"PERSON\",\"Bob\"],[\"COMMERCIAL_ITEM\",\"Pixel 5\"]]",
            "[[\"PERSON\",\"Jim\"],[\"COMMERCIAL_ITEM\",\"Pixel 2XL\"]]",
            "[[\"PERSON\",\"Rob\"]]"
        };
        System.out.println(mergeEntities(arrayOfJsonArrays)); 
        int maxTextBytes = 70;
        String longText = "My name is Jeremiah. I live in Anytown, USA. I am 35 years old. I am 5'7\" tall. I love cars, and dogs. My SSN is 123-45-6789. My cell is (707)555-1234.";
        String[] arrayOfJsonStrings = splitLongText(longText, maxTextBytes);
        System.out.println(mergeText(arrayOfJsonStrings));  
    }
    
    static String[] makeArray(String text, int len)
    {
        String[] textArray = new String[len];
        for (int i = 0; i < len; i++) {
            textArray[i] = text;
        } 
        return textArray;
    }
    
    static void functional_tests() throws Exception
    {
        TextAnalyticsUDFHandler textAnalyticsUDFHandler = new TextAnalyticsUDFHandler();

        System.out.println("\nSPLIT LONG TEXT BLOCKS");
        runSplitLongTextTest();
        
        System.out.println("\nTEXT SPLITTING INTO SENTENCES");
        runSplitBySentenceTests(textAnalyticsUDFHandler);

        System.out.println("\nUTF-8 STRING LENGTH TESTS");
        runStringLengthTests();
        
        System.out.println("\nMERGE RESULTS TESTS");
        runMergeEntitiesTests();
        
        String[] text;
        String[] lang;

        String result;
        System.out.println("\nDETECT DOMINANT LANGUAGE");
        text = new String[]{"I am Bob", "Je m'appelle Bob"};
        // check logs for evidence of 1 batch with 2 items
        System.out.println("detect_dominant_language - 2 rows:" + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_dominant_language(text)));
        System.out.println("detect_dominant_language_all - 2 rows:" + text);
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_dominant_language_all(text)));
        
        System.out.println("\nDETECT SENTIMENT");
        text = new String[]{"I am happy", "She is sad", "ce n'est pas bon", "Je l'aime beaucoup"};
        lang = new String[]{"en", "en", "fr", "fr"};
        // check logs for evidence of 2 batches with 2 items each, grouped by lang
        System.out.println("detect_sentiment - 4 rows: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_sentiment(text, lang)));  
        System.out.println("detect_sentiment_all - 4 rows: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_sentiment_all(text, lang)));
        
        System.out.println("\nDETECT / REDACT ENTITIES");
        text = new String[]{"I am Bob, I live in Herndon", "Je suis Bob et j'habite à Herndon", "Soy Bob y vivo en Herndon"};
        lang = new String[]{"en", "fr", "es"};
        System.out.println("detect_entities - 3 rows: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_entities(text, lang)));
        System.out.println("detect_entities_all - 3 rows: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_entities_all(text, lang)));   
        System.out.println("redact_entities - 3 rows, types ALL: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.redact_entities(text, lang, makeArray("ALL", 3)))); 
        
        System.out.println("\nDETECT / REDACT PII ENTITIES");
        text = new String[]{"I am Bob, I live in Herndon"};
        lang = new String[]{"en"};
        System.out.println("detect_pii_entities - 1 row: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_pii_entities(text, lang)));
        System.out.println("detect_pii_entities_all - 1 row: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_pii_entities_all(text, lang)));   
        System.out.println("redact_pii_entities - 1 row, types ALL: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.redact_pii_entities(text, lang, makeArray("ALL", 3)))); 

        System.out.println("\nDETECT KEY PHRASES");
        text = new String[]{"I really enjoyed the book, Of Mice and Men, by John Steinbeck"};
        lang = new String[]{"en"};
        System.out.println("detect_key_phrases - 1 row: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_key_phrases(text, lang)));
        System.out.println("detect_key_phrases_all - 1 row: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_key_phrases_all(text, lang))); 

        System.out.println("\nTRANSLATE TEXT");
        text = new String[]{"I am Bob, I live in Herndon", "I love to visit France"};
        String[] sourcelang = new String[]{"en", "en"};
        String[] targetlang = new String[]{"fr", "fr"};
        String[] terminologyNames = new String[]{"null", "null"};
        System.out.println("translate_text - 2 rows: " + toJSON(text));
        System.out.println(toJSON(textAnalyticsUDFHandler.translate_text(text, sourcelang, targetlang, terminologyNames)));

        System.out.println("\nLONG TEXT TESTS");
        int textBytes = 60;
        int batchSize = 3; 
        textAnalyticsUDFHandler.maxTextBytes = textBytes;
        textAnalyticsUDFHandler.maxBatchSize = batchSize;
        System.out.println("Set max text length to " + textBytes + " bytes, and max batch size to " + batchSize + ", for testing");
        text = new String[]{"I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon."};
        lang = new String[]{"en"};
        System.out.println("detect_sentiment - 1 row: " + toJSON(text));
        System.out.println("check logs for evidence of long text truncated by detect_sentiment.");
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_sentiment(text, lang)));
        text = new String[]{"I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon."};
        System.out.println("detect_entities / redact_entities - 1 row: " + toJSON(text));
        System.out.println("check logs for evidence of long text split into 2 batches w/ max 3 rows per batch.");
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_entities(text, lang)));        
        System.out.println(toJSON(textAnalyticsUDFHandler.redact_entities(text, lang, makeArray("ALL", 1))));        
        System.out.println("detect_pii_entities / redact_pii_entities - 1 row: " + toJSON(text));
        System.out.println("check logs for evidence of long text split into 3 rows.");
        text = new String[]{"I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon."};
        System.out.println(toJSON(textAnalyticsUDFHandler.detect_pii_entities(text, lang)));        
        System.out.println(toJSON(textAnalyticsUDFHandler.redact_pii_entities(text, lang, makeArray("ALL", 1))));        
    }

    static void performance_tests() throws Exception
    {
        TextAnalyticsUDFHandler textAnalyticsUDFHandler = new TextAnalyticsUDFHandler();
        System.out.println("\nDETECT ENTITIES - BATCH PERFORMANCE TEST");
        int size = 7500;
        String[] text;
        String[] lang;
        text = makeArray("I am Bob, I live in Herndon", size);
        lang = makeArray("en", size);
        textAnalyticsUDFHandler.detect_entities(text, lang);
        //textAnalyticsUDFHandler.detect_pii_entities(text, lang);
        System.out.println("\nDONE");
    }

    // java -cp target/textanalyticsudfs-1.0.jar com.amazonaws.athena.udf.textanalytics.TextAnalyticsUDFHandler
    public static void main(String[] args) throws Exception
    {
        functional_tests();
        //performance_tests();
    }
}
