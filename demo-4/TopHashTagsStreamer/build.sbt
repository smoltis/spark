name := "Top HashTags Streamer"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.4.0"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).
                                    value.copy(includeScala = false)