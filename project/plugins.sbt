// You may use this file to add plugin dependencies for sbt.

// According to https://spark.apache.org/news/new-repository-service.html, changing resolver
resolvers += "spark-packages" at "https://repos.spark-packages.org/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.1.1")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")

addSbtPlugin("com.databricks" %% "sbt-databricks" % "0.1.3")

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
