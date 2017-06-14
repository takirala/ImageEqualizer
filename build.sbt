name := "ImageEqualizer"

version := "1.0"

scalaVersion := "2.12.1"

libraryDependencies += "org.apache.mesos" % "mesos" % "1.2.1"

unmanagedResourceDirectories in Compile += { baseDirectory.value / "images/" }

assemblyOutputPath in assembly := file("/Users/takirala/git/dcos/dcos-vagrant/ImageEqualizer-assembly-1.0.jar")