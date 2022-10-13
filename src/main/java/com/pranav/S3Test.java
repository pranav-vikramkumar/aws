package com.pranav;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Test implements RequestHandler<Map<String, String>, String> {

	@Override
	public String handleRequest(Map<String, String> input, Context context) {

		S3Client client = S3Client.builder()
				.region(Region.US_EAST_1)
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build();

		// List buckets
		List<Bucket> buckets = client.listBuckets()
				.buckets();
		for (Bucket bucket : buckets) {
			System.out.println(String.format("Bucket name is %s, objects are :", bucket.name()));

			List<S3Object> objects = client.listObjects(request -> request.bucket(bucket.name())
					.build())
					.contents();

			// List objects
			objects.forEach(object -> {
				System.out.println(object.key() + object.size() + object.owner()
						.displayName());
			});
		}
		return "Hi";

		/*
		 * // Upload to bucket s3client.putObject( "pranav-bucket",
		 * "myprefix/uploaded_sample.txt", new File("sample.txt") );
		 * 
		 */
	}
}
