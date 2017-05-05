name := "spark-crimes"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Artima Maven Repository" at  "http://repo.artima.com/releases"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.1" exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.1" exclude("org.scalatest", "scalatest_2.11")

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test"

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

