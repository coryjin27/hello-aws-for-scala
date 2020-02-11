import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object HelloDynamoDBApp extends App {

  println("Hello, world!")

  def createTable(name: String, keys: List[(String, ScalarAttributeType, KeyType)], readCapacityUnits: Long = 1L, writeCapacityUnits: Long = 1L)(implicit ddb: DynamoDbClient): CreateTableResponse = {
    val attributeDefinitions = keys
      .map({
        case (name, attr_type, _) =>
          AttributeDefinition.builder().attributeName(name).attributeType(attr_type).build()
      })
      .asJava

    val keySchemaElements = keys
        .map({
          case (name, _, key_type) =>
            KeySchemaElement.builder().attributeName(name).keyType(key_type).build()
        })
        .asJava

    ddb.createTable(
      CreateTableRequest.builder()
        .tableName(name)
        .attributeDefinitions(attributeDefinitions)
        .keySchema(keySchemaElements)
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(readCapacityUnits).writeCapacityUnits(writeCapacityUnits).build())
        .build()
    )
  }
  def getTableStatus(name: String)(implicit ddb: DynamoDbClient) = {
    ddb.describeTable(DescribeTableRequest.builder().tableName(name).build()).table().tableStatus()
  }
  def deleteTable(name: String)(implicit ddb: DynamoDbClient): DeleteTableResponse = {
    ddb.deleteTable(DeleteTableRequest.builder().tableName(name).build())
  }

  def putItem(table: String, values: Map[String, AttributeValue])(implicit ddb: DynamoDbClient): PutItemResponse = {
    ddb.putItem(PutItemRequest.builder().tableName(table).item(values.asJava).build())
  }
  def updateItem(table: String, key: Map[String, AttributeValue], values: Map[String, AttributeValueUpdate])(implicit ddb: DynamoDbClient): UpdateItemResponse = {
    ddb.updateItem(UpdateItemRequest.builder().tableName(table).key(key.asJava).attributeUpdates(values.asJava).build())
  }
  def getItem(table: String, key: Map[String, AttributeValue])(implicit ddb: DynamoDbClient): GetItemResponse = {
    ddb.getItem(GetItemRequest.builder().tableName(table).key(key.asJava).build())
  }
  def deleteItem(table: String, key: Map[String, AttributeValue])(implicit ddb: DynamoDbClient): DeleteItemResponse = {
    ddb.deleteItem(DeleteItemRequest.builder().tableName(table).key(key.asJava).build())
  }

  implicit val ddb: DynamoDbClient = DynamoDbClient.builder().build()

  val table = "test-table-" + System.currentTimeMillis()
  val keys = List(
    ("key", ScalarAttributeType.S, KeyType.HASH)
  )

  val item1_key = Map("key" -> AttributeValue.builder().s("key_1").build())
  val item1_value1 = Map("msg" -> AttributeValue.builder().s("Hello, AWS DynamoDB!").build())
  val item1_1 = item1_key ++ item1_value1

  val item1_value2 = Map("msg" -> AttributeValue.builder().s("Hello, Amazon DynamoDB!").build())
  val item1_value2_update = item1_value2.mapValues(av => AttributeValueUpdate.builder().action(AttributeAction.PUT).value(av).build())
  val item1_2 = item1_key ++ item1_value2

  println(s"Creating a new DynamoDB table... (table=$table, keys=${keys.map(_._1)})")

  Try(createTable(table, keys)) match {
    case Success(_) =>
      println(s"|-- Create table succeeded.")

      //

      println(s"Waiting for the table got activated...")

      while ({
        println(s"|-- Describing the table...")

        Try(getTableStatus(table)) match {
          case Success(status) =>
            println(s"    |-- Get table status succeeded. (status=$status)")
            status

          case Failure(ex) =>
            println("    |-- Get table status failed: ")
            ex.printStackTrace()
        }

      } != TableStatus.ACTIVE) {
        Thread.sleep(1000 * 1)
      }

      println(s"|-- The table is now in active.")

      //

      println(s"Putting a new item... (item1_1=$item1_1)")

      Try(putItem(table, item1_1)) match {
        case Success(_) =>
          println(s"|-- Put item succeeded.")

          //

          println(s"Updating the item... (item1_key=$item1_key, item1_value2_update=$item1_value2_update)")

          Try(updateItem(table, item1_key, item1_value2_update)) match {
            case Success(_) =>
              println(s"|-- Update item succeeded.")

              //

              println(s"Getting the item...")

              Try(getItem(table, item1_key)) match {
                case Success(resp) =>
                  println(s"|-- Get item succeeded.")

                  val item_get = resp.item().asScala
                  if (item1_2.mapValues(_.toString) == item_get.mapValues(_.toString)) {
                    println(item_get("msg").s())

                  } else {
                    println("Unmatching item attributes: ")
                    println(s"|-- expected:\t$item1_2")
                    println(s"|-- actual:\t$item_get")
                  }

                case Failure(ex) =>
                  println("|-- Get item failed: ")
                  ex.printStackTrace()
              }

            case Failure(ex) =>
              println("|-- Update item failed: ")
              ex.printStackTrace()
          }

          //

          println(s"Deleting the item...")

          Try(deleteItem(table, item1_key)) match {
            case Success(_) =>
              println(s"|-- Delete item succeeded.")

            case Failure(ex) =>
              println("|-- Delete item failed: ")
              ex.printStackTrace()
          }

        case Failure(ex) =>
          println("|-- Put item failed: ")
          ex.printStackTrace()
      }

      //

      println(s"Deleting the table...")

      Try(deleteTable(table)) match {
        case Success(_) =>
          println(s"|-- Delete table succeeded.")

        case Failure(ex) =>
          println("|-- Delete table failed: ")
          ex.printStackTrace()
      }

    case Failure(ex) =>
      println("|-- Create table failed: ")
      ex.printStackTrace()
  }

}
