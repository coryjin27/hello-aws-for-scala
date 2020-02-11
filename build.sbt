name := "hello-aws-for-scala"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions += "-target:jvm-1.8"

//

// https://mvnrepository.com/artifact/software.amazon.awssdk/s3
libraryDependencies += "software.amazon.awssdk" % "s3" % "2.10.60"

// https://mvnrepository.com/artifact/software.amazon.awssdk/dynamodb
libraryDependencies += "software.amazon.awssdk" % "dynamodb" % "2.10.60"
