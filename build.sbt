name := "spark-crimes"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "Artima Maven Repository" at  "http://repo.artima.com/releases"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.3.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.1" exclude("org.scalatest", "scalatest_2.11")

//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
//libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.3.1_0.3.3" % "test"

libraryDependencies += "org.pegdown" % "pegdown" % "1.6.0" % "test"

parallelExecution in Test := false

val htmlReportParameters = Seq (
  Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports"),
  Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports"),
  Tests.Argument(TestFrameworks.ScalaTest, "-o")
)
val htmlReport = System.getProperty("htmlReport")

testOptions in Test ++= (
  // generate reports if the property is defined: -DhtmlReport=true
  if ("true" == htmlReport) htmlReportParameters else Seq()
)

