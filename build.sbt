
lazy val compilerFlags = Seq(
  scalacOptions ++= (
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n <= 11 => // for 2.11 all we care about is capabilities, not warnings
        Seq(
          "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
          "-language:higherKinds",             // Allow higher-kinded types
          "-language:implicitConversions",     // Allow definition of implicit functions called views
          "-Ypartial-unification"              // Enable partial unification in type constructor inference
        )
      case _ =>
        Seq(
          "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
          "-encoding", "utf-8",                // Specify character encoding used by source files.
          "-explaintypes",                     // Explain type errors in more detail.
          "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
          "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
          "-language:higherKinds",             // Allow higher-kinded types
          "-language:implicitConversions",     // Allow definition of implicit functions called views
          "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
          "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
          "-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
          "-Xfuture",                          // Turn on future language features.
          "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
          "-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
          "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
          "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
          "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
          "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
          "-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
          "-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
          "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
          "-Xlint:option-implicit",            // Option.apply used implicit view.
          "-Xlint:package-object-classes",     // Class or object defined in package object.
          "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
          "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
          "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
          "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
          // "-Yno-imports",                      // No predef or default imports
          "-Ywarn-dead-code",                  // Warn when dead code is identified.
          "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
          "-Ywarn-numeric-widen",              // Warn when numerics are widened.
          "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
          //"-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
          "-Ywarn-unused:locals",              // Warn if a local definition is unused.
          //"-Ywarn-unused:params",              // Warn if a value parameter is unused.
          "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
          "-Ywarn-unused:privates"            // Warn if a private member is unused.
//          "-Ywarn-value-discard"               // Warn when non-Unit expression results are unused.
        )
    }
    ),
  // flags removed in 2.13
  scalacOptions ++= (
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n == 12 =>
        Seq(
          "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
          "-Ypartial-unification"              // Enable partial unification in type constructor inference
        )
      case _ =>
        Seq.empty
    }
    ),
  scalacOptions in (Test, compile) --= (
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n <= 11 =>
        Seq("-Yno-imports")
      case _ =>
        Seq(
          "-Ywarn-unused:privates",
          "-Ywarn-unused:locals",
          "-Ywarn-unused:imports",
          "-Yno-imports"
        )
    }
    ),
  scalacOptions in (Compile, console) --= Seq("-Xfatal-warnings", "-Ywarn-unused:imports", "-Yno-imports"),
  scalacOptions in (Compile, console) ++= Seq("-Ydelambdafy:inline"), // http://fs2.io/faq.html
  scalacOptions in (Compile, doc)     --= Seq("-Xfatal-warnings", "-Ywarn-unused:imports", "-Yno-imports")
)

lazy val commonSettings = compilerFlags ++ Seq(
  name := "export",
  version := "1.0.0-SNAPSHOT",
  scalaVersion := "2.12.6",
)
lazy val buildSettings = Seq(
  organization := "org.tpolecat",
  licenses ++= Seq(("MIT", url("http://opensource.org/licenses/MIT")))
)

lazy val doobieSettings = buildSettings ++ commonSettings

lazy val DoobieVersion = "0.7.0-M5"
lazy val FlywayVersion = "5.2.4"
lazy val PureConfigVersion = "0.10.2"
lazy val LogbackVersion = "1.2.3"
lazy val ScalaTestVersion = "3.0.5"
lazy val ScalaMockVersion = "4.1.0"
lazy val HiveVersion = "2.1.0"
lazy val hadoopmapreduceclientcommonversion = "2.7.2"
lazy val hadoopcommonversion = "2.7.2"
lazy val snakeyamlversion = "1.23"
lazy val argotversion = "1.0.3"
lazy val httpcoreversion = "4.4.10"
lazy val httpclientversion = "4.5.6"
lazy val libthriftversion = "0.9.2"
lazy val protobufjavaversion = "2.5.0"
lazy val kindProjectorVersion = "0.9.9"
lazy val fs2Version           = "1.0.4"

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    commonSettings,
    Defaults.itSettings,
    libraryDependencies ++= Seq(

      "org.tpolecat"          %% "doobie-core"          % DoobieVersion,
      "org.tpolecat"          %% "doobie-h2"            % DoobieVersion,
      "org.tpolecat"          %% "doobie-hikari"        % DoobieVersion,


      "com.github.pureconfig" %% "pureconfig"           % PureConfigVersion,

      "ch.qos.logback"        %  "logback-classic"      % LogbackVersion,

      //Hive
      "org.apache.hive"       % "hive-jdbc"             % HiveVersion,
      "org.apache.hive"       %  "hive-common"          % HiveVersion,
      "org.apache.hive"       %  "hive-exec"          % HiveVersion,
      "org.apache.hive"       %  "hive-metastore"          % HiveVersion,
      "org.apache.hive"       %  "hive-service"          % HiveVersion,
      //Oracle
      "oracle"              % "ojdbc7" % "12.1.0.1",
      //Hadoop
      "org.apache.hadoop"     % "hadoop-mapreduce-client-common" % hadoopmapreduceclientcommonversion,
      "org.apache.hadoop"     % "hadoop-common"         % hadoopcommonversion % "provided",
      "org.apache.hadoop"     % "hadoop-annotations"    % hadoopcommonversion,

      //Cats
      "co.fs2"         %% "fs2-core"    % fs2Version,
      "org.typelevel"         %% "cats-core"            % "1.6.0",
      "org.typelevel"         %% "cats-effect"          % "1.3.0",
      "org.typelevel"         %% "cats-free"          % "1.6.0",

      "com.github.pureconfig" %% "pureconfig"           % "0.10.1",
      "com.chuusai"           %% "shapeless"            % "2.3.3",
      "com.github.scopt"      %% "scopt"                % "3.7.1",

      //Others
      "org.pentaho"               % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde" % Test,
      "eigenbase"                 % "eigenbase-properties"          % "1.1.4",
      "org.apache.httpcomponents" % "httpclient"        % httpclientversion,
      "org.apache.thrift"         % "libthrift"         % libthriftversion,
      "com.google.protobuf"       % "protobuf-java"     % protobufjavaversion,
      //Testing
      "org.scalatest"         %% "scalatest"            % ScalaTestVersion  % "it,test",
      "org.scalamock"         %% "scalamock"            % ScalaMockVersion  % "test"
    )
  )


resolvers += Resolver.mavenLocal
resolvers += "Cascading repo" at "http://conjars.org/repo"
resolvers in Global += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
ensimeIgnoreScalaMismatch in ThisBuild := true

addCompilerPlugin("org.spire-math" %% "kind-projector" % kindProjectorVersion)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
