package cromwell.client

import cromwell.client.parser.CromwellClientInterfaceParser
import cromwell.client.parser.CromwellClientInterfaceParser.Submit
import org.slf4j.LoggerFactory

object CromwellClientEntryPoint extends App with CromwellEntryPointHelper {
  
  val logger = LoggerFactory.getLogger("cromwell-submit")

  //  cromwell 29
  //  Usage: java -jar /path/to/cromwell-client.jar submit [options] <args>...
  //
  //    --help                   Cromwell - Workflow Execution Engine
  //    --version
  //  Command: submit [options] workflow-source
  //  Submit the workflow to the cromwell server described by the host argument.
  //  workflow-source          Workflow source file.
  //  -i, --inputs <value>     Workflow inputs file.
  //  -o, --options <value>    Workflow options file.
  //  -t, --type <value>       Workflow type.
  //  -v, --type-version <value>
  //                           Workflow type version.
  //  -l, --labels <value>     Workflow labels file.
  //  -p, --imports <value>    A directory or zipfile to search for workflow imports.
  //  -m, --metadata-output <value> An optional directory path to output metadata.
  //  -h, --host <value> URL of Cromwell server
  def buildParser(): scopt.OptionParser[CommandLineArguments] = new CromwellClientInterfaceParser

  def runCromwell(args: CommandLineArguments): Unit = {
    args.command match {
      case Some(Submit) => submitToServer(args)
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
