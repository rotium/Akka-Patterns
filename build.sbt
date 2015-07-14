name := """akka-pattern"""
organization := "com.rotium.akka"
version := "1.0"

licenses += ("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html"))
homepage := Some(url("https://github.com/rotium/Akka-Patterns"))

scmInfo := Some(ScmInfo(url("https://github.com/rotium/Akka-Patterns.git"), "scm:git:git@github.com:rotium/Akka-Patterns.git"))

pomExtra := 
  <developers>
    <developer>
      <id>rotem</id>
      <name>Rotem Erlich</name>
      <url>https://github.com/rotium</url>
    </developer>
  </developers>

scalaVersion := "2.11.6"
crossScalaVersions := Seq("2.10.4", "2.11.4")
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

EclipseKeys.withSource := true
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.6",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.6" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test")
  
