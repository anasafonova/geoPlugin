name := "geoPlugin"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.jcenterRepo

/* Comment out whichever geotrellis-* modules you don't need */
libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-proj4"  % "3.5.0" % Provided
)

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.1.0" % Compile