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

package com.amazonaws.athena.connectors.textanalytics;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder;
import com.amazonaws.services.comprehend.model.BatchDetectDominantLanguageItemResult;
import com.amazonaws.services.comprehend.model.BatchDetectDominantLanguageRequest;
import com.amazonaws.services.comprehend.model.BatchDetectDominantLanguageResult;
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesItemResult;
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult;
import com.amazonaws.services.comprehend.model.BatchDetectSentimentItemResult;
import com.amazonaws.services.comprehend.model.BatchDetectSentimentRequest;
import com.amazonaws.services.comprehend.model.BatchDetectSentimentResult;
import com.amazonaws.services.comprehend.model.BatchItemError;
import com.amazonaws.services.comprehend.model.DetectPiiEntitiesRequest;
import com.amazonaws.services.comprehend.model.DetectPiiEntitiesResult;
import com.amazonaws.services.comprehend.model.Entity;
import com.amazonaws.services.comprehend.model.PiiEntity;
import com.amazonaws.services.comprehend.model.SentimentScore;
import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.AmazonTranslateClientBuilder;
import com.amazonaws.services.translate.model.TranslateTextRequest;
import com.amazonaws.services.translate.model.TranslateTextResult;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TextAnalyticsUDFHandler 
        extends UserDefinedFunctionHandler
{
    
    private static final String SOURCE_TYPE = "athena_textanalytics_udf";
    public static int maxTextBytes = 5000;  //utf8 bytes
    public static int maxBatchSize = 25;
    
    private AmazonComprehend comprehendClient;
    private AmazonTranslate translateClient;

    private ClientConfiguration createClientConfiguration()
    {
        int retryBaseDelay = 1000;
        int retryMaxBackoffTime = 600000;
        int maxRetries = 100;
        RetryPolicy retryPolicy = new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                                                  new PredefinedBackoffStrategies.ExponentialBackoffStrategy(retryBaseDelay, retryMaxBackoffTime),
                                                  maxRetries,
                                                  false);
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                                                    .withRequestTimeout(5000)
                                                    .withRetryPolicy(retryPolicy);
        return clientConfiguration;
    }
    private AmazonComprehend getComprehendClient() 
    {
        // create client first time on demand
        String region = (System.getenv("AWS_REGION") != null) ? System.getenv("AWS_REGION") : "us-east-1";
        AWSCredentialsProvider awsCreds = DefaultAWSCredentialsProviderChain.getInstance();
        if (this.comprehendClient == null) {
            System.out.println("Creating Comprehend client connection with ExponentialBackoffStrategy");
            ClientConfiguration clientConfiguration = createClientConfiguration();
            this.comprehendClient = AmazonComprehendClientBuilder.standard()
                                             .withCredentials(awsCreds)
                                             .withRegion(region)
                                             .withClientConfiguration(clientConfiguration)
                                             .build();
            }
        return this.comprehendClient;
    }

    private AmazonTranslate getTranslateClient() 
    {
        // create client first time on demand
        String region = (System.getenv("AWS_REGION") != null) ? System.getenv("AWS_REGION") : "us-east-1";
        AWSCredentialsProvider awsCreds = DefaultAWSCredentialsProviderChain.getInstance();
        if (this.translateClient == null) {
            System.out.println("Creating Translate client connection with ExponentialBackoffStrategy");
            ClientConfiguration clientConfiguration = createClientConfiguration();
            this.translateClient = AmazonTranslateClientBuilder.standard()
                                             .withCredentials(awsCreds)
                                             .withRegion(region)
                                             .withClientConfiguration(clientConfiguration)
                                             .build();
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

    public String detect_dominant_language(String inputjson) throws Exception
    {
        return detect_dominant_language(inputjson, false);
    }
    public String detect_dominant_language_all(String inputjson) throws Exception
    {
        return detect_dominant_language(inputjson, true);
    }   
    private String detect_dominant_language(String inputjson, boolean fullResponse) throws Exception
    {
        // convert input string to array
        String[] input = fromJSON(inputjson);
        
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
            BatchDetectDominantLanguageRequest batchDetectDominantLanguageRequest = new BatchDetectDominantLanguageRequest().withTextList(textArray);
            BatchDetectDominantLanguageResult batchDetectDominantLanguageResult = getComprehendClient().batchDetectDominantLanguage(batchDetectDominantLanguageRequest);
            // Throw exception if errorList is populated
            List<BatchItemError> batchItemError = batchDetectDominantLanguageResult.getErrorList();
            if (! batchItemError.isEmpty()) {
                throw new RuntimeException("Error:  - ErrorList in batchDetectDominantLanguage result: " + batchItemError);
            }
            List<BatchDetectDominantLanguageItemResult> batchDetectDominantLanguageItemResult = batchDetectDominantLanguageResult.getResultList(); 
            for (int i = 0; i < batchDetectDominantLanguageItemResult.size(); i++) {
                if (fullResponse) {
                    // return JSON structure containing array of all detected languageCodes and scores
                    result[rowNum] = this.toJSON(batchDetectDominantLanguageItemResult.get(i).getLanguages());
                }
                else {
                    // return simple string containing the languageCode of the first (most confident) language
                    result[rowNum] = batchDetectDominantLanguageItemResult.get(i).getLanguages().get(0).getLanguageCode();
                }
                rowNum++;
            }
        }
        // Convert output array to JSON string
        String resultjson = toJSON(result);
        return resultjson;
    }

    /**
     * DETECT SENTIMENT
     * ================
     **/

    public String detect_sentiment(String inputjson, String languagejson) throws Exception
    {
        return detect_sentiment(inputjson, languagejson, false);
    }    
    public String detect_sentiment_all(String inputjson, String languagejson) throws Exception
    {
        return detect_sentiment(inputjson, languagejson, true);
    }   
    private String detect_sentiment(String inputjson, String languagejson, boolean fullResponse) throws Exception
    {
        // convert input args to arrays
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
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
            BatchDetectSentimentRequest batchDetectSentimentRequest = new BatchDetectSentimentRequest().withTextList(textArray).withLanguageCode(languageCode);
            BatchDetectSentimentResult batchDetectSentimentResult = getComprehendClient().batchDetectSentiment(batchDetectSentimentRequest);
            // Throw exception if errorList is populated
            List<BatchItemError> batchItemError = batchDetectSentimentResult.getErrorList();
            if (! batchItemError.isEmpty()) {
                throw new RuntimeException("Error:  - ErrorList in batchDetectSentiment result: " + batchItemError);
            }
            List<BatchDetectSentimentItemResult> batchDetectSentimentItemResult = batchDetectSentimentResult.getResultList(); 
            for (int i = 0; i < batchDetectSentimentItemResult.size(); i++) {
                if (fullResponse) {
                    // return JSON structure containing array of all sentiments and scores
                    String sentiment = batchDetectSentimentItemResult.get(i).getSentiment();
                    SentimentScore sentimentScore = batchDetectSentimentItemResult.get(i).getSentimentScore();
                    result[rowNum] = "{\"sentiment\":" + toJSON(sentiment) + ",\"sentimentScore\":" + toJSON(sentimentScore) + "}";
                }
                else {
                    // return simple string containing the main sentiment
                    result[rowNum] = batchDetectSentimentItemResult.get(i).getSentiment();
                }
                rowNum++;
            }                
        }
        // Convert output array to JSON string
        String resultjson = toJSON(result);
        return resultjson;
    }

    /**
     * DETECT ENTITIES
     * ================
     **/

    public String detect_entities(String inputjson, String languagejson) throws Exception
    {
        return detect_entities(inputjson, languagejson, "[]", false);
    }    
    public String detect_entities_all(String inputjson, String languagejson) throws Exception
    {
        return detect_entities(inputjson, languagejson, "[]", true);
    }  
    public String redact_entities(String inputjson, String languagejson, String redacttypesjson) throws Exception
    {
        return detect_entities(inputjson, languagejson, redacttypesjson, false);
    }
    private String detect_entities(String inputjson, String languagejson, String redacttypesjson, boolean fullResponse) throws Exception
    {
        // convert input args to arrays
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        String[] redactTypesArray = fromJSON(redacttypesjson);
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
        // Convert output array to JSON string
        String resultjson = toJSON(result);
        return resultjson;
    }

    private String[] MultiRowBatchDetectEntities(String languageCode, String[] batch, String[] redactTypes, boolean fullResponse) throws Exception
    {
        String[] result = new String[batch.length];
        // Call batchDetectEntities API
        BatchDetectEntitiesRequest batchDetectEntitiesRequest = new BatchDetectEntitiesRequest().withTextList(batch).withLanguageCode(languageCode);
        BatchDetectEntitiesResult batchDetectEntitiesResult = getComprehendClient().batchDetectEntities(batchDetectEntitiesRequest);
        // Throw exception if errorList is populated
        List<BatchItemError> batchItemError = batchDetectEntitiesResult.getErrorList();
        if (! batchItemError.isEmpty()) {
            throw new RuntimeException("Error:  - ErrorList in batchDetectEntities result: " + batchItemError);
        }
        List<BatchDetectEntitiesItemResult> batchDetectEntitiesItemResult = batchDetectEntitiesResult.getResultList(); 
        if (batchDetectEntitiesItemResult.size() != batch.length) {
            throw new RuntimeException("Error:  - batch size and result item count do not match");
        }
        for (int i = 0; i < batchDetectEntitiesItemResult.size(); i++) {
            List<Entity> entities = batchDetectEntitiesItemResult.get(i).getEntities();
            if (fullResponse) {
                // return JSON structure containing all entity types, scores and offsets
                result[i] = this.toJSON(entities);
            }
            else {
                if (redactTypes.length == 0) {
                    // no redaction - return JSON string containing the entity types and extracted values
                    result[i] = getEntityTypesAndValues(entities, batch[i]);                      
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
            BatchDetectEntitiesRequest batchDetectEntitiesRequest = new BatchDetectEntitiesRequest().withTextList(textArray).withLanguageCode(languageCode);
            BatchDetectEntitiesResult batchDetectEntitiesResult = getComprehendClient().batchDetectEntities(batchDetectEntitiesRequest);
            // Throw exception if errorList is populated
            List<BatchItemError> batchItemError = batchDetectEntitiesResult.getErrorList();
            if (! batchItemError.isEmpty()) {
                throw new RuntimeException("Error:  - ErrorList in batchDetectEntities result: " + batchItemError);
            }
            List<BatchDetectEntitiesItemResult> batchDetectEntitiesItemResult = batchDetectEntitiesResult.getResultList(); 
            if (batchDetectEntitiesItemResult.size() != textArray.length) {
                throw new RuntimeException("Error:  - array size " + textArray.length + " and result item count " + batchDetectEntitiesItemResult.size() + " do not match");
            }
            int cumOffset = 0;
            for (int i = 0; i < batchDetectEntitiesItemResult.size(); i++) {
                List<Entity> entities = batchDetectEntitiesItemResult.get(i).getEntities();
                if (fullResponse) {
                    // return JSON structure containing all entity types, scores and offsets
                    result[rowNum] = this.toJSON(entities);
                }
                else {
                    if (redactTypes.equals("")) {
                        // no redaction - return JSON string containing the entity types and extracted values
                        result[rowNum] = getEntityTypesAndValues(entities, textArray[i]);                      
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
    private String getEntityTypesAndValues(List<Entity> entities, String text) throws Exception
    {
        List<String[]> typesAndValues = new ArrayList<String[]>();
        for (Entity entity : entities) {
            String type = entity.getType();
            String value = text.substring(entity.getBeginOffset(), entity.getEndOffset());
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
            String type = entity.getType();
            if (redactTypes.contains(type) || redactTypes.contains("ALL")) {
                // this is a PII type we need to redact
                // Offset logic assumes piiEntity list is ordered by occurance in string
                int start = entity.getBeginOffset() + deltaLength;
                int end = entity.getEndOffset() + deltaLength;
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
     
    public String detect_pii_entities(String inputjson, String languagejson) throws Exception
    {
        return detect_pii_entities(inputjson, languagejson, "[]", false);
    }    
    public String detect_pii_entities_all(String inputjson, String languagejson) throws Exception
    {
        return detect_pii_entities(inputjson, languagejson, "[]", true);
    }  
    public String redact_pii_entities(String inputjson, String languagejson, String redacttypesjson) throws Exception
    {
        return detect_pii_entities(inputjson, languagejson, redacttypesjson, false);
    }
    private String detect_pii_entities(String inputjson, String languagejson, String redacttypesjson, boolean fullResponse) throws Exception
    {
        // convert input args to arrays
        String[] input = fromJSON(inputjson);
        String[] languageCodes = fromJSON(languagejson);
        String[] redactTypesArray = fromJSON(redacttypesjson);
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
        // Convert output array to JSON string
        String resultjson = toJSON(result);
        return resultjson;
    }
    private String[] MultiRowBatchDetectPiiEntities(String languageCode, String[] batch, String[] redactTypes, boolean fullResponse) throws Exception
    {
        String[] result = new String[batch.length];
        // Call detectPiiEntities API in loop  (no multidocument batch API available)
        for (int i = 0; i < batch.length; i++) {
            DetectPiiEntitiesRequest detectPiiEntitiesRequest = new DetectPiiEntitiesRequest().withText(batch[i]).withLanguageCode(languageCode);
            DetectPiiEntitiesResult detectPiiEntitiesResult = getComprehendClient().detectPiiEntities(detectPiiEntitiesRequest);
            List<PiiEntity> piiEntities = detectPiiEntitiesResult.getEntities(); 
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
            DetectPiiEntitiesRequest detectPiiEntitiesRequest = new DetectPiiEntitiesRequest().withText(batch[i]).withLanguageCode(languageCode);
            DetectPiiEntitiesResult detectPiiEntitiesResult = getComprehendClient().detectPiiEntities(detectPiiEntitiesRequest);
            List<PiiEntity> piiEntities = detectPiiEntitiesResult.getEntities(); 
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
            String type = piiEntity.getType();
            String value = text.substring(piiEntity.getBeginOffset(), piiEntity.getEndOffset());
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
            String type = piiEntity.getType();
            if (redactTypes.contains(type) || redactTypes.contains("ALL")) {
                // this is a PII type we need to redact
                // Offset logic assumes piiEntity list is ordered by occurance in string
                int start = piiEntity.getBeginOffset() + deltaLength;
                int end = piiEntity.getEndOffset() + deltaLength;
                int length1 = result.length(); 
                result = new String(result.substring(0, start) + "[" + type + "]" + result.substring(end));
                deltaLength = deltaLength + (result.length() - length1);
            }
        }
        return result;
    }
    
    /**
     * TRANSLATE TEXT
     */
    public String translate_text(String inputjson, String sourcelanguagejson, String targetlanguagejson, String terminologynamesjson) throws Exception
    {
        // convert input args to arrays
        String[] input = fromJSON(inputjson);
        String[] sourceLanguageCodes = fromJSON(sourcelanguagejson);
        String[] targetLanguageCodes = fromJSON(targetlanguagejson);
        String[] terminologyNames = fromJSON(terminologynamesjson);
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
        // Convert output array to JSON string
        String resultjson = toJSON(result);
        return resultjson;
    }
    private String[] MultiRowBatchTranslateText(String[] batch, String[] sourceLanguageCodesSubset, String[] targetLanguageCodesSubset, String[] terminologyNamesSubset) throws Exception
    {
        String[] result = new String[batch.length];
        // Call translateText API in loop  (no multidocument batch API available)
        for (int i = 0; i < batch.length; i++) {
            TranslateTextRequest translateTextRequest = new TranslateTextRequest()
                                                                .withText(batch[i])
                                                                .withSourceLanguageCode(sourceLanguageCodesSubset[i])
                                                                .withTargetLanguageCode(targetLanguageCodesSubset[i]);
            if (! terminologyNamesSubset[i].equals("null")) {
                translateTextRequest = translateTextRequest.withTerminologyNames(terminologyNamesSubset[i]);
            }
            try {
                TranslateTextResult translateTextResult = getTranslateClient().translateText(translateTextRequest);
                String translatedText = translateTextResult.getTranslatedText();  
                result[i] = translatedText;
            } 
            catch (Exception e) {
                System.out.println("ERROR: Translate API Exception.\nInput String size: " + getUtf8StringLength(batch[i]) + " bytes. String:\n" + batch[i]);
                throw e;
            }
        }
        return result;
    }
    private String TextSplitBatchTranslateText(String[] batch, String sourceLanguageCode, String targetLanguageCode, String terminologyName) throws Exception
    {
        String[] result = new String[batch.length];
        // Call translateText API in loop  (no multidocument Translate API available)
        for (int i = 0; i < batch.length; i++) {
            TranslateTextRequest translateTextRequest = new TranslateTextRequest()
                                                                .withText(batch[i])
                                                                .withSourceLanguageCode(sourceLanguageCode)
                                                                .withTargetLanguageCode(targetLanguageCode);
            if (! terminologyName.equals("null")) {
                translateTextRequest = translateTextRequest.withTerminologyNames(terminologyName);
            }
            try {
                TranslateTextResult translateTextResult = getTranslateClient().translateText(translateTextRequest);
                String translatedText = translateTextResult.getTranslatedText();  
                result[i] = translatedText;
            } 
            catch (Exception e) {
                System.out.println("ERROR: Translate API Exception.\nInput String size: " + getUtf8StringLength(batch[i]) + " bytes. String:\n" + batch[i]);
                throw e;
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
        String result;
        System.out.println("Test splitting text by sentence");
        String longText = "My name is Mr. P. A. Jeremiah Smith Jr., and I live at 1234 Summer Dr., Anytown, USA. This sentence has 10.5 words, and some abbreviations, e.g. this one. Also: punctuation in quotes, like this, \"Way to go Joe!\", she said.";
        System.out.println("Original text: " + longText);
        result = textAnalyticsUDFHandler.redact_pii_entities(makeJsonArray(longText, 1), makeJsonArray("en", 1), makeJsonArray("ALL", 1));
        System.out.println("Original - PII Redacted: " + String.join("", fromJSON(result))); 
        result = textAnalyticsUDFHandler.redact_entities(makeJsonArray(longText, 1), makeJsonArray("en", 1), makeJsonArray("ALL", 1));
        System.out.println("Original - Entities Redacted: " + String.join("", fromJSON(result))); 
        String[] sentenceArray = splitStringBySentence(longText);
        System.out.println("Split sentences: \n" + String.join("\n", sentenceArray)); 
        int cnt = sentenceArray.length;
        result = textAnalyticsUDFHandler.redact_pii_entities(toJSON(sentenceArray), makeJsonArray("en", cnt), makeJsonArray("ALL", cnt));
        result = String.join("", fromJSON(result));
        System.out.println("Text Split, PII Redacted and combined: " + result); 
        result = textAnalyticsUDFHandler.redact_entities(toJSON(sentenceArray), makeJsonArray("en", cnt), makeJsonArray("ALL", cnt));
        result = String.join("", fromJSON(result));
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
    
    static String makeJsonArray(String text, int len)
    {
        String[] textArray = new String[len];
        for (int i = 0; i < len; i++) {
            textArray[i] = text;
        } 
        return toJSON(textArray);
    }
    
    public static void main(String[] args) throws Exception
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
        
        String textJSON;
        String langJSON;

        String result;
        System.out.println("\nDETECT DOMINANT LANGUAGE");
        textJSON = toJSON(new String[]{"I am Bob", "Je m'appelle Bob"});
        // check logs for evidence of 1 batch with 2 items
        System.out.println("detect_dominant_language - 2 rows:" + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_dominant_language(textJSON));
        System.out.println("detect_dominant_language_all - 2 rows:" + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_dominant_language_all(textJSON));
        
        System.out.println("\nDETECT SENTIMENT");
        textJSON = toJSON(new String[]{"I am happy", "She is sad", "ce n'est pas bon", "Je l'aime beaucoup"});
        langJSON = toJSON(new String[]{"en", "en", "fr", "fr"});
        // check logs for evidence of 2 batches with 2 items each, grouped by lang
        System.out.println("detect_sentiment - 4 rows: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_sentiment(textJSON, langJSON));  
        System.out.println("detect_sentiment_all - 4 rows: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_sentiment_all(textJSON, langJSON));
        
        System.out.println("\nDETECT / REDACT ENTITIES");
        textJSON = toJSON(new String[]{"I am Bob, I live in Herndon", "Je suis Bob et j'habite à Herndon", "Soy Bob y vivo en Herndon"});
        langJSON = toJSON(new String[]{"en", "fr", "es"});
        System.out.println("detect_entities - 3 rows: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_entities(textJSON, langJSON));
        System.out.println("detect_entities_all - 3 rows: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_entities_all(textJSON, langJSON));   
        System.out.println("redact_entities - 3 rows, types ALL: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.redact_entities(textJSON, langJSON, makeJsonArray("ALL", 3))); 
        
        System.out.println("\nDETECT / REDACT PII ENTITIES");
        textJSON = toJSON(new String[]{"I am Bob, I live in Herndon"});
        langJSON = toJSON(new String[]{"en"});
        System.out.println("detect_pii_entities - 1 row: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_pii_entities(textJSON, langJSON));
        System.out.println("detect_pii_entities - 1 row: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.detect_pii_entities_all(textJSON, langJSON));   
        System.out.println("redact_pii_entities - 1 row, types ALL: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.redact_pii_entities(textJSON, langJSON, makeJsonArray("ALL", 3))); 

        System.out.println("\nTRANSLATE TEXT");
        textJSON = toJSON(new String[]{"I am Bob, I live in Herndon", "I love to visit France"});
        String sourcelangJSON = toJSON(new String[]{"en", "en"});
        String targetlangJSON = toJSON(new String[]{"fr", "fr"});
        String terminologyNamesJSON = toJSON(new String[]{"null", "null"});
        System.out.println("translate_text - 1 row: " + textJSON);
        System.out.println(textAnalyticsUDFHandler.translate_text(textJSON, sourcelangJSON, targetlangJSON, terminologyNamesJSON));

        System.out.println("\nLONG TEXT TESTS");
        int textBytes = 60;
        int batchSize = 3; 
        textAnalyticsUDFHandler.maxTextBytes = textBytes;
        textAnalyticsUDFHandler.maxBatchSize = batchSize;
        System.out.println("Set max text length to " + textBytes + " bytes, and max batch size to " + batchSize + ", for testing");
        textJSON = toJSON(new String[]{"I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon. I am Bob, I live in Herndon."});
        langJSON = toJSON(new String[]{"en"});
        System.out.println("detect_sentiment - 1 row: " + textJSON);
        System.out.println("check logs for evidence of long text truncated by detect_sentiment.");
        System.out.println(textAnalyticsUDFHandler.detect_sentiment(textJSON, langJSON));
        System.out.println("detect_entities / redact_entities - 1 row: " + textJSON);
        System.out.println("check logs for evidence of long text split into 2 batches w/ max 3 rows per batch.");
        System.out.println(textAnalyticsUDFHandler.detect_entities(textJSON, langJSON));        
        System.out.println(textAnalyticsUDFHandler.redact_entities(textJSON, langJSON, makeJsonArray("ALL", 1)));        
        System.out.println("detect_pii_entities / redact_pii_entities - 1 row: " + textJSON);
        System.out.println("check logs for evidence of long text split into 3 rows.");
        System.out.println(textAnalyticsUDFHandler.detect_pii_entities(textJSON, langJSON));        
        System.out.println(textAnalyticsUDFHandler.redact_pii_entities(textJSON, langJSON, makeJsonArray("ALL", 1)));        
        
    }
}
