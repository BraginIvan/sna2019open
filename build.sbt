name := "sna2019"

version := "0.1"

scalaVersion := "2.11.11"


assemblyOption in assembly ~= { _.copy(includeScala = false).copy(includeDependency = false) }

//mainClass in assembly := Some("ru.sociohub.Matcher")

libraryDependencies += "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2" exclude("ch.qos.logback", "logback-classic")
//
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"

libraryDependencies += "joda-time" % "joda-time" % "2.10.1"

test in assembly := {}

assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList("com","hadoop", xs @ _*) => MergeStrategy.discard
    case PathList("org","hsqldb", xs @ _*) => MergeStrategy.discard
    case PathList("org","jboss", xs @ _*) => MergeStrategy.discard
    case PathList("org","mortbay", xs @ _*) => MergeStrategy.discard
    case PathList("org","objectweb", xs @ _*) => MergeStrategy.discard
    case PathList("org","objenesis", xs @ _*) => MergeStrategy.discard
    case PathList("org","slf4j", xs @ _*) => MergeStrategy.last
    case PathList("org","znerd", xs @ _*) => MergeStrategy.discard
    case PathList("thrift", xs @ _*) => MergeStrategy.discard
    case PathList("junit", xs @ _*) => MergeStrategy.discard
    case PathList("org", "apache","jasper", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache","commons", xs @ _*) => MergeStrategy.first
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.discard
    case "application.conf" => MergeStrategy.concat
    case "reference.conf" => MergeStrategy.concat
    case "unwanted.txt"     => MergeStrategy.discard
    // case "project.clj" => MergeStrategy.discard // Leiningen build files
    case x if x.endsWith(".txt") => MergeStrategy.first
    case x if x.contains("META-INF") => MergeStrategy.discard
    case x if x.endsWith(".html") => MergeStrategy.first // More bumf
    case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last // For Log$Logger.class
    case x =>
      MergeStrategy.first
    //old(x)
  }
}