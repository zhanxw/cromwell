package cromwell.client.parser

import common.util.VersionUtil
import cromwell.client.CommandLineArguments
import cromwell.client.parser.BaseCromwellCommandLineParser._
import cromwell.core.path.DefaultPathBuilder

object BaseCromwellCommandLineParser {
  lazy val cromwellVersion = VersionUtil.getVersion("cromwell")
}

abstract class BaseCromwellCommandLineParser extends scopt.OptionParser[CommandLineArguments]("java -jar /path/to/cromwell.jar") {
  protected val workflowSubmissionParameters = List(
    arg[String]("workflow-source").text("Workflow source file.").required().
      action((s, c) => c.copy(workflowSource = Option(DefaultPathBuilder.get(s)))),
    opt[String]("workflow-root").text("Workflow root.").
      action((s, c) =>
        c.copy(workflowRoot = Option(s))),
    opt[String]('i', "inputs").text("Workflow inputs file.").
      action((s, c) =>
        c.copy(workflowInputs = Option(DefaultPathBuilder.get(s)))),
    opt[String]('o', "options").text("Workflow options file.").
      action((s, c) =>
        c.copy(workflowOptions = Option(DefaultPathBuilder.get(s)))),
    opt[String]('t', "type").text("Workflow type.").
      action((s, c) =>
        c.copy(workflowType = Option(s))),
    opt[String]('v', "type-version").text("Workflow type version.").
      action((s, c) =>
        c.copy(workflowTypeVersion = Option(s))),
    opt[String]('l', "labels").text("Workflow labels file.").
      action((s, c) =>
        c.copy(workflowLabels = Option(DefaultPathBuilder.get(s)))),
    opt[String]('p', "imports").text(
      "A directory or zipfile to search for workflow imports.").
      action((s, c) =>
        c.copy(imports = Option(DefaultPathBuilder.get(s)))),
    opt[String]('m', "metadata-output").text(
      "An optional directory path to output metadata.").
      action((s, c) =>
        c.copy(metadataOutput = Option(DefaultPathBuilder.get(s))))
  )

  head("cromwell", cromwellVersion)

  help("help").text("Cromwell - Workflow Execution Engine")

  version("version")
}
