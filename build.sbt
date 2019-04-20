name := "DistributedPrimeFinder"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.22",
  "com.typesafe.akka" %% "akka-remote" % "2.5.22"
)

libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.8.1"
