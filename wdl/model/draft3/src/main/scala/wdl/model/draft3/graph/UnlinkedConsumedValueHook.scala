package wdl.model.draft3.graph

import wom.types.WomType

sealed trait UnlinkedConsumedValueHook

final case class UnlinkedIdentifierHook(name: String) extends UnlinkedConsumedValueHook

/**
  * Hook representing unlinked call inputs. We'll satisfy these by creating input nodes.
  */
final case class UnlinkedCallInputHook(callReference: String,
                                       inputName: String,
                                       inputType: WomType) extends UnlinkedConsumedValueHook

/**
  * Until we do the linking, we can't tell whether a consumed 'x.y' is a call output or a member access for 'y' on
  * a variable called 'x'.
  */
final case class UnlinkedCallOutputOrIdentifierAndMemberAccessHook(name: String,
                                                                   firstLookup: String) extends UnlinkedConsumedValueHook
