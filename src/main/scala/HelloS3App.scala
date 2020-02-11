import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import scala.util.{Failure, Success, Try}

object HelloS3App extends App {

  println("Hello, world!")

  def createBucket(name: String)(implicit s3: S3Client): CreateBucketResponse = {
    s3.createBucket(CreateBucketRequest.builder().bucket(name).build())
  }
  def deleteBucket(name: String)(implicit s3: S3Client): DeleteBucketResponse = {
    s3.deleteBucket(DeleteBucketRequest.builder().bucket(name).build())
  }

  def putObject(bucket: String, key: String, content: Array[Byte])(implicit s3: S3Client): PutObjectResponse = {
    s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromBytes(content))
  }
  def getObject(bucket: String, key: String)(implicit s3: S3Client): ResponseBytes[GetObjectResponse] = {
    s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build())
  }
  def deleteObject(bucket: String, key: String)(implicit s3: S3Client): DeleteObjectResponse = {
    s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build())
  }

  implicit val s3: S3Client = S3Client.builder().build()

  val bucket = "test-bucket-" + System.currentTimeMillis()
  val object_key = "test-dir-1/test-dir-2/test-obj-" + System.currentTimeMillis() + ".txt"
  val object_content = "Hello, Amazon S3!"

  println(s"Creating a new S3 bucket... (bucket=$bucket)")

  Try(createBucket(bucket)) match {
    case Success(_) =>
      println(s"|-- Create bucket succeeded.")

      //

      println(s"Putting a new S3 object... (object_key=$object_key, object_content='$object_content')")

      val content_put = object_content.getBytes()
      Try(putObject(bucket, object_key, content_put)) match {
        case Success(_) =>
          println(s"|-- Put object succeeded.")

          //

          println(s"Getting the object...")

          Try(getObject(bucket, object_key)) match {
            case Success(resp) =>
              println(s"|-- Get object succeeded.")

              val content_get = resp.asByteArray()
              if (content_put.toList == content_get.toList) {
                println(new String(content_get))

              } else {
                println("Unmatching object contents: ")
                println(s"|-- expected:\t${new String(content_put)}")
                println(s"|-- actual:\t${new String(content_get)}")
              }

            case Failure(ex) =>
              println("|-- Get object failed: ")
              ex.printStackTrace()
          }

          //

          println(s"Deleting the object...")

          Try(deleteObject(bucket, object_key)) match {
            case Success(_) =>
              println(s"|-- Delete object succeeded.")

            case Failure(ex) =>
              println("|-- Delete object failed: ")
              ex.printStackTrace()
          }

        case Failure(ex) =>
          println("|-- Put object failed: ")
          ex.printStackTrace()
      }

      //

      println(s"Deleting the bucket...")

      Try(deleteBucket(bucket)) match {
        case Success(_) =>
          println(s"|-- Delete bucket succeeded.")

        case Failure(ex) =>
          println("|-- Delete bucket failed: ")
          ex.printStackTrace()
      }

    case Failure(ex) =>
      println("|-- Create bucket failed: ")
      ex.printStackTrace()
  }

}
