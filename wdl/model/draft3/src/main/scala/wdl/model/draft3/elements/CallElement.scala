package wdl.model.draft3.elements

/**
  * @param callableReference The (maybe qualified) name of the callable to call
  * @param alias An alias for this call
  * @param body The coll body
  */
final case class CallElement(callableReference: String, alias: Option[String], body: Option[CallBodyElement])
  extends LanguageElement with WorkflowGraphElement {

  /**
    * The call reference by which this call can be referred to in the rest of the workflow
    */
  def callReference: String = alias.getOrElse(callableReference.split("\\.").last)
}
