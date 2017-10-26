name := "spark-exam"

version := "1.0"

scalaVersion := "2.11.11"

resolvers += "Artima Maven Repository" at  "http://repo.artima.com/releases"

libraryDependencies += "org.apache.spark" %% "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx_2.11" % "2.2.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.4" % "test"

libraryDependencies += "org.pegdown" % "pegdown" % "1.6.0" % "test"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

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

