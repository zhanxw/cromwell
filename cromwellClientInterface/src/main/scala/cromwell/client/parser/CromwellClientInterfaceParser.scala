package cromwell.client.parser

import java.net.URL

import cromwell.client.parser.CromwellClientInterfaceParser.Submit

object CromwellClientInterfaceParser {
  val Submit = "Submit"
}

class CromwellClientInterfaceParser extends BaseCromwellCommandLineParser {
  val submitParameter = workflowSubmissionParameters ++ List(
    opt[String]('h', "host").text("Workflow server host. Only used for the submit command").
      action((h, c) =>
        c.copy(host = new URL(h))
      )
  )

  // Add a submit command with an optional host parameter
  cmd("submit")
    .action((_, c) => c.copy(command = Option(Submit)))
    .text("Submit the workflow to a Cromwell server.")
    .children(submitParameter: _*)
}
