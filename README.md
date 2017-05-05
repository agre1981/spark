# Spark Crimes

## Compilation
```
sbt package
```

## Run tests
```
sbt test
```

## Run tests with html report generation 
```
sbt "-DhtmlReport=true" test
```

## Run tests with coverage
```
sbt clean coverage test
```

## generate coverage report
```
sbt coverageReport
```
report is in target/scala-2.11/scoverage-report

## Show sbt dependency tree
```
sbt dependencyTree >dependency.txt
```

