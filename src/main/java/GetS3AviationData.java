import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class GetS3AviationData {

    private AmazonS3 s3;
    private KafkaProducer producer = null;
    private String topic = "aviation-dataset";


    private final static Logger LOGGER = Logger.getLogger(GetS3AviationData.class);

    public GetS3AviationData(String awsKeyId, String awsAccessKey, KafkaProducerClient client) {

        AWSCredentials credentials = new BasicAWSCredentials(awsKeyId, awsAccessKey);
        this.producer = client.producer;

        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.EU_WEST_2)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        LOGGER.info("Getting s3 buckets");

        final List<Bucket> buckets = s3.listBuckets();

        Bucket usbucket = null;
        for (Bucket bucket : buckets) {
            if (bucket.getName().contains("us-aviation-data")) {
                usbucket = bucket;
                break;
            }
        }

        if (usbucket != null) {
            final ObjectListing listing = s3.listObjects(usbucket.getName());
            System.out.println(listing.getObjectSummaries());
            List<S3ObjectSummary> dataBuckets = getDatasetObjects(listing.getObjectSummaries());
            System.out.println("s3 buckets summmaries " + dataBuckets);
            getObjectsAndSendToKafka(s3, dataBuckets, usbucket);
        }

    }


    private void getObjectsAndSendToKafka(AmazonS3 s3, List<S3ObjectSummary> s3objects, Bucket bucket) {
        for (S3ObjectSummary s3object : s3objects) {
            GetObjectRequest objectRequest = new GetObjectRequest(bucket.getName(), s3object.getKey());
            final S3Object objectResponse = s3.getObject(objectRequest);
            try {
                sendToKafkaTopic(objectResponse.getObjectContent());
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.warn("Failed to get s3 object: "  + s3object.getKey());
            }
        }
    }

    private List<S3ObjectSummary> getDatasetObjects(List<S3ObjectSummary> buckets) {
        List<S3ObjectSummary> fBuckers = new ArrayList<>();
        for (S3ObjectSummary bucket : buckets) {
            if (bucket.getKey().startsWith("online-perf/")) {
                fBuckers.add(bucket);
            }
        }
        return fBuckers;
    }

    private void sendToKafkaTopic(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;

        while ((line = reader.readLine()) != null) {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            final ProducerRecord<Long, String> record = new ProducerRecord<>(topic, timestamp.getTime(), line);
            try {
                producer.send(record).get();
                LOGGER.info("Sending line: " + line + " to topic: " + topic);
            } catch (InterruptedException e) {
                e.printStackTrace();
                LOGGER.warn(e);
            } catch (ExecutionException e) {
                e.printStackTrace();
                LOGGER.warn(e);
            }
        }

    }
}
