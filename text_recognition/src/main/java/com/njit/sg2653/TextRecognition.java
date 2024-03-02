package com.njit.sg2653;

// Author: Christian Carpena
// CS643-852 Programming Assignment 1

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.util.*;
import java.io.*;

public class TextRecognition {

    public static void main(String[] args) {

        String bucketName = "njit-cs-643";
        String queueName = "car.fifo"; 

        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create("ASIAQ3EGRBRTKQ6JHUJJ", "y6JzUKuaR3E71qRzojjaZgy6tqcM/1onkyg6V6Bm");
        Region region = Region.US_EAST_1;

        S3Client s3 = S3Client.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
        .region(region)
        .build();
        RekognitionClient rek = RekognitionClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
        .region(region)
        .build();
        SqsClient sqs = SqsClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
        .region(region)
        .build();


        processCarImages(s3, rek, sqs, bucketName, queueName);
    }

    public static void processCarImages(S3Client s3, RekognitionClient rek, SqsClient sqs, String bucketName,
                                        String queueName) {

        boolean QExists = false;
        while (!QExists) {
            ListQueuesRequest ReqQList = ListQueuesRequest.builder()
                    .queueNamePrefix(queueName)
                    .build();
            ListQueuesResponse ResQList = sqs.listQueues(ReqQList);
            if (ResQList.queueUrls().size() > 0)
                QExists = true;
        }

        String queueUrl = "";
        try {
            GetQueueUrlRequest getReqQ = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            queueUrl = sqs.getQueueUrl(getReqQ)
                    .queueUrl();
        } catch (QueueNameExistsException e) {
            throw e;
        }

        try {
            boolean endOfQ = false;
            HashMap<String, String> outputs = new HashMap<String, String>();

            while (!endOfQ) {
                // Retrieve next image index
                ReceiveMessageRequest MsgReqRx = ReceiveMessageRequest.builder().queueUrl(queueUrl)
                        .maxNumberOfMessages(1).build();
                List<Message> messages = sqs.receiveMessage(MsgReqRx).messages();

                if (messages.size() > 0) {
                    Message message = messages.get(0);
                    String label = message.body();

                    if (label.equals("-1")) {
                        
                        endOfQ = true;
                    } else {
                        System.out.println("Processing car image with text from njit-cs-643 S3 bucket: " + label);

                        Image img = Image.builder().s3Object(S3Object.builder().bucket(bucketName).name(label).build())
                                .build();
                        DetectTextRequest request = DetectTextRequest.builder()
                                .image(img)
                                .build();
                        DetectTextResponse result = rek.detectText(request);
                        List<TextDetection> textDetections = result.textDetections();

                        if (textDetections.size() != 0) {
                            String text = "";
                            for (TextDetection textDetection : textDetections) {
                                if (textDetection.type().equals(TextTypes.WORD))
                                    text = text.concat(" " + textDetection.detectedText());
                            }
                            outputs.put(label, text);
                        }
                    }

                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteMessageRequest);
                }
            }
            try {
                FileWriter writer = new FileWriter("output.txt");

                Iterator<Map.Entry<String, String>> it = outputs.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, String> pair = it.next();
                    writer.write(pair.getKey() + ":" + pair.getValue() + "\n");
                    it.remove();
                }

                writer.close();
                System.out.println("Results written to file output.txt");
            } catch (IOException e) {
                System.out.println("An error occurred writing to file.");
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }
}
