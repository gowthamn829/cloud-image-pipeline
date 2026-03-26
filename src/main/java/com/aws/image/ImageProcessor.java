package com.aws.image;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

public class ImageProcessor implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final AmazonSNS snsClient = AmazonSNSClientBuilder.defaultClient();

    String outputBucket = "image-processing-pipeline-gowtham";
    String snsTopicArn = "arn:aws:sns:ap-south-1:014195622314:image-upload-alert";

    @Override
    public String handleRequest(S3Event event, Context context) {

        try {

            
            String bucketName = event.getRecords().get(0).getS3().getBucket().getName();

            String key = java.net.URLDecoder.decode(
                    event.getRecords().get(0).getS3().getObject().getKey(),
                    "UTF-8"
            );

            context.getLogger().log("Bucket: " + bucketName);
            context.getLogger().log("File uploaded: " + key);

            // ✅ Process only input-images folder
            if (key.startsWith("input-images/")) {

                String fileName = key.substring(key.lastIndexOf("/") + 1);
                String newKey = "processed-images/" + fileName;

                context.getLogger().log("Copying file to: " + newKey);

                // ✅ Copy file to processed-images folder
                s3Client.copyObject(bucketName, key, outputBucket, newKey);

                context.getLogger().log("File copied successfully");

// 🔥 ADD THIS LINE
context.getLogger().log("About to send SNS notification");

// ✅ Send SNS notification
snsClient.publish(
        snsTopicArn,
        "Image processed successfully: " + fileName,
        "Image Processing Notification"
);

context.getLogger().log("SNS notification sent");
            } else {
                context.getLogger().log("File not in input-images folder. Skipping...");
            }

        } catch (Exception e) {

            context.getLogger().log("ERROR: " + e.getMessage());
        }

        return "Done";
    }
}