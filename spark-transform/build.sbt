name := "spark-transform"

version := "0.1"

scalaVersion := "2.12.0"
val sparkVersion = "3.0.0"

libraryDependencies ++={
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion

    //dependency for local mongodb test
    //    "org.mongodb"                 %%  "casbah"                      % "2.8.1"     % "test",
    //    "de.flapdoodle.embed"         %   "de.flapdoodle.embed.mongo"   % "2.2.0"    % "test",
    //    "com.github.simplyscala"      %%  "scalatest-embedmongo"        % "0.2.2"     % "test",
    //    "org.scalatest"       	      %%  "scalatest"             	    % "2.2.4"     % "test",

  )
}

