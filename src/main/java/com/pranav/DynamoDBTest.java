package com.pranav;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.pranav.model.TestTable;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;

public class DynamoDBTest implements RequestHandler<Map<String, String>, String> {
	@Override
	public String handleRequest(Map<String, String> input, Context context) {

		DynamoDbClient client = DynamoDbClient.builder()
				.credentialsProvider(DefaultCredentialsProvider.create())
				.region(Region.US_EAST_1)
				.build();
		DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
				.dynamoDbClient(client)
				.build();
		DynamoDbTable<TestTable> table = enhancedClient.table("test_table", TableSchema.fromClass(TestTable.class));

		System.out.println("Describing table : ");
		System.out.println(client.describeTable(DescribeTableRequest.builder()
				.tableName("test_table")
				.build())
				.table()
				.attributeDefinitions());

		// Add or update

		// V1 uses an inbuilt request object called Item
		// Item item1 = new Item();
		// item1.withNumber("id", 1);
		// item1.withString("name", "Pranav");
		// item1.withNumber("age", 20);
		// testTable.putItem(item1);

		// V2 supports only batch write. Supports only POJOs
		TestTable item1 = new TestTable();
		item1.setId(1);
		item1.setName("Pranav");
		item1.setAge(20);
		table.putItem(request -> request.item(item1)
				.build());

		// Conditional update - Either Get and Update or Update with expression

		// V1 uses an inbuilt request object called updateItemSpec
		// testTable.updateItem(new UpdateItemSpec().withPrimaryKey("id", 1)
		// .withUpdateExpression("SET adult = :isAdult")
		// .withConditionExpression("age > :minAge")
		// .withValueMap(new ValueMap().withBoolean(":isAdult", true)
		// .withInt(":minAge", 18)));

		// V2 supports the POJO. Use ignoreNulls for patch, else it will replace all
		TestTable item2 = new TestTable();
		item2.setId(1);
		item2.setIsAdult(true);
		table.updateItem(request -> request.item(item2)
				.ignoreNulls(true)
				.conditionExpression(Expression.builder()
						.expression("age > :minAge")
						.expressionValues(Map.of(":minAge", AttributeValue.fromN(String.valueOf(18))))
						.build())
				.build());

		// V1 - Uses GetItemSpec. It support projection etc
		// Name is a reserved keyword, thus cannot be directly used in any projection or
		// filter expression. Thus use 2 step substitution

		// Item receivedItem = testTable.getItem(new GetItemSpec().withPrimaryKey("id",
		// 1)
		// .withProjectionExpression("id,#n,age")
		// .withNameMap(Map.of("#n", "name"))
		// .withConsistentRead(true));
		// System.out.println(
		// String.format("Got %s is %d years old", receivedItem.getString("name"),
		// receivedItem.getInt("age")));

		// V2 does not support projection. Only entire object
		TestTable receivedItem = table.getItem(request -> request.key(Key.builder()
				.partitionValue(1)
				.build())
				.consistentRead(true));
		System.out.println(String.format("Got %s is %d years old", receivedItem.getName(), receivedItem.getAge()));

		// Query
		// Key condition expression MUST have 1 partition key equals, and one or more
		// sort key conditions. Only sort key conditions can include >,< etc

		// V1 - Uses Query Spec. Supports projection
		// ItemCollection<QueryOutcome> queriedItems = testTable
		// .query(new QuerySpec().withKeyConditionExpression("id = :partitionkeyvalue")
		// .withProjectionExpression("id,#n,age")
		// .withValueMap(Map.of(":partitionkeyvalue", 1))
		// .withNameMap(Map.of("#n", "name"))
		// .withConsistentRead(true));
		// queriedItems.forEach(item -> {
		// System.out.println(String.format("Queried %s is %d years old",
		// receivedItem.getString("name"),
		// receivedItem.getInt("age")));
		// });

		// V2 does not support projection. Entire object
		SdkIterable<TestTable> queriedItems = table
				.query(request -> request.queryConditional(QueryConditional.keyEqualTo(Key.builder()
						.partitionValue(1)
						.build()))
						.consistentRead(true))
				.items();
		queriedItems.forEach(item -> {
			System.out.println(String.format("Queried %s is %d years old", item.getName(), receivedItem.getAge()));
		});

		return "Complete";
	}
}
