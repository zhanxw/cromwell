package cromwell.services

import cromwell.core.actor.ThrottlerActor
import cromwell.services.instrumentation.{CromwellInstrumentationActor, InstrumentedBatchActor}
import cromwell.services.loadcontroller.LoadControlledBatchActor

abstract class EnhancedThrottlerActor[C]
  extends ThrottlerActor[C]
  with InstrumentedBatchActor[C] 
  with CromwellInstrumentationActor
  with LoadControlledBatchActor[C]
