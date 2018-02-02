package cromwell

import common.util.VersionUtil
import cromwell.client.CommandLineArguments
import cromwell.client.parser.CromwellClientInterfaceParser
import cromwell.client.parser.CromwellClientInterfaceParser.Submit

object CommandLineParser extends App {
  lazy val Run = "Run"
  lazy val Server = "Server"

  lazy val cromwellVersion = VersionUtil.getVersion("cromwell")

  //  cromwell 29
  //  Usage: java -jar /path/to/cromwell.jar [server|run] [options] <args>...
  //
  //    --help                   Cromwell - Workflow Execution Engine
  //    --version
  //  Command: server
  //  Starts a web server on port 8000.  See the web server documentation for more details about the API endpoints.
  //  Command: run [options] workflow-source
  //  Run the workflow and print out the outputs in JSON format.
  //  workflow-source          Workflow source file.
  //  -i, --inputs <value>     Workflow inputs file.
  //  -o, --options <value>    Workflow options file.
  //  -t, --type <value>       Workflow type.
  //  -v, --type-version <value>
  //                           Workflow type version.
  //  -l, --labels <value>     Workflow labels file.
  //  -p, --imports <value>    A directory or zipfile to search for workflow imports.
  //  -m, --metadata-output <value>
  //                           An optional directory path to output metadata.

  def buildParser(): scopt.OptionParser[CommandLineArguments] = {
    new CromwellClientInterfaceParser {
      cmd("server").action((_, c) => c.copy(command = Option(Server))).text(
        "Starts a web server on port 8000.  See the web server documentation for more details about the API endpoints.")

      cmd("run")
        .action((_, c) => c.copy(command = Option(Run)))
        .text("Run the workflow and print out the outputs in JSON format.")
        .children(workflowSubmissionParameters: _*)
    }
  }

  def runCromwell(args: CommandLineArguments): Unit = {
    args.command match {
      case Some(Run) => CromwellEntryPoint.runSingle(args)
      case Some(Server) => CromwellEntryPoint.runServer()
      case Some(Submit) => CromwellEntryPoint.submitToServer(args)
      case _ => parser.showUsage()
    }
  }

  val parser = buildParser()

  val parsedArgs = parser.parse(args, CommandLineArguments())
  parsedArgs match {
    case Some(pa) => runCromwell(pa)
    case None => parser.showUsage()
  }
}
