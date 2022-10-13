package com.pranav;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

public class ParameterTest implements RequestHandler<Map<String, String>, Void> {

	@Override
	public Void handleRequest(Map<String, String> input, Context context) {
		SsmClient ssmClient = SsmClient.builder()
				.credentialsProvider(DefaultCredentialsProvider.create())
				.region(Region.US_EAST_1)
				.build();

		String username = ssmClient.getParameter(GetParameterRequest.builder()
				.name("db-username")
				.build())
				.parameter()
				.value();

		String password = ssmClient.getParameter(GetParameterRequest.builder()
				.name("db-password")
				.withDecryption(true)
				.build())
				.parameter()
				.value();

		System.out.println(String.format("Username = %s, Password = %s", username, password));

		return null;
	}
}
