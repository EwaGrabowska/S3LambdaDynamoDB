package lambda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public class Main implements RequestHandler<S3Event, String> {
    private AmazonDynamoDB amazonDynamoDB;
    private final String DYNAMODB_TABLE_NAME = "argo1";
    private final Regions REGION = Regions.EU_CENTRAL_1;

    @Override
    public String handleRequest(S3Event event, Context context)
    {

        LambdaLogger logger = context.getLogger();
        logger.log("CONTEXT: " + context.getFunctionName());

        String key = event.getRecords().get(0).getS3().getObject().getKey();
        logger.log("KEY: "+ key);

        String backet = event.getRecords().get(0).getS3().getBucket().getName();
        logger.log("BACKET: "+ backet);


        File file = downloadFileFromS3(backet, key);
        String jsonResult = persistData(file, key);
        logger.log("JSON: "+jsonResult);

        return "{ \"message\": \"200\" }";
    }

    private File downloadFileFromS3(String backet, String key) {
        S3Downloader s3Downloader = new S3Downloader();
        File file = s3Downloader.downloadFromS3(backet, key);
        return file;
    }

    String persistData(File file, String key) throws ConditionalCheckFailedException {
        initDynamoDbClient();
        String jsonResult ="";
        NetcdfReader netcdfReader = new NetcdfReader();
        try {
            Map<String, AttributeValue> attributesMap = netcdfReader.persistData(file, key);
            PutItemResult result = amazonDynamoDB.putItem(DYNAMODB_TABLE_NAME, attributesMap);
            ObjectMapper objectMapper = new ObjectMapper();
            jsonResult = objectMapper.writeValueAsString(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return jsonResult;
    }
    private void initDynamoDbClient() {
        this.amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(REGION)
                .build();
    }
}
