name := "spark-transform"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++={
  Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0",
    "org.apache.spark" %% "spark-sql" % "2.4.0",
//    "org.mongodb"                 %%  "casbah"                      % "2.8.1"     % "test",
//    "de.flapdoodle.embed"         %   "de.flapdoodle.embed.mongo"   % "2.2.0"    % "test",
//    "com.github.simplyscala"      %%  "scalatest-embedmongo"        % "0.2.2"     % "test",
//    "org.scalatest"       	      %%  "scalatest"             	    % "2.2.4"     % "test",
    "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"

  )
}

