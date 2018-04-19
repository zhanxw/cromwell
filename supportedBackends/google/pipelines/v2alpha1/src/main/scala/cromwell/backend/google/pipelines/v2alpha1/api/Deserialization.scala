package cromwell.backend.google.pipelines.v2alpha1.api

import java.time.OffsetDateTime
import java.util.{ArrayList => JArrayList, Map => JMap}

import com.google.api.client.json.GenericJson
import com.google.api.services.genomics.v2alpha1.model.{Event, Operation, Pipeline, WorkerAssignedEvent}
import cromwell.core.ExecutionEvent
import mouse.all._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * This bundles up some ugliness to work around the fact that Operation is not deserialized
  * completely.
  * For instance, the metadata field is a Map[String, Object] even though it represents "Metadata"
  * for which there's an existing class.
  * This class provides implicit functions to deserialize those map to their proper type.
  */
private [api] object Deserialization {
  implicit class EnhancedEvent(val event: Event) extends AnyVal {
    /**
      * Attempts to deserialize the details map to T
      * Returns None if the details are not of type T
      */
    def details[T <: GenericJson](implicit tag: ClassTag[T]): Option[T] = {
      val detailsMap = event.getDetails
      // The @type field contains the type of the attribute
      if (hasDetailsClass(tag)) {
        Option(deserializeTo(detailsMap)(tag))
      } else None
    }
    
    def hasDetailsClass[T <: GenericJson](implicit tag: ClassTag[T]): Boolean = {
      event.getDetails.asScala("@type").asInstanceOf[String].endsWith(tag.runtimeClass.getSimpleName)
    }

    def toExecutionEvent = ExecutionEvent(event.getDescription, OffsetDateTime.parse(event.getTimestamp))
  }

  implicit class EnhancedOperation(val operation: Operation) extends AnyVal {
    /**
      * Deserializes the events to com.google.api.services.genomics.v2alpha1.model.Event
      */
    def events: List[Event] = operation
      .getMetadata.asScala("events").asInstanceOf[JArrayList[JMap[String, Object]]]
      .asScala.toList
      .map(deserializeTo[Event])

    /**
      * Deserializes the pipeline to com.google.api.services.genomics.v2alpha1.model.Pipeline
      */
    def pipeline: Pipeline = operation
      .getMetadata.asScala("pipeline").asInstanceOf[JMap[String, Object]] |> deserializeTo[Pipeline]

    def hasStarted = events.exists(_.hasDetailsClass[WorkerAssignedEvent])
  }

  /**
    * Deserializes a java.util.Map[String, Object] to an instance of T
    */
  private def deserializeTo[T <: GenericJson](attributes: JMap[String, Object])(implicit tag: ClassTag[T]): T = {
    // Create a new instance, because it's a GenericJson there's always a 0-arg constructor
    val newT = tag.runtimeClass.asInstanceOf[Class[T]].newInstance()

    // Optionally returns the field with the given name
    def field(name: String) = Option(newT.getClassInfo.getField(name))
    
    def handleMap(key: String, value: Object) = {
      (field(key), value) match {
        // If both the value can be assigned directly to the field, just do that
        case (Some(f), _) if f.getType.isAssignableFrom(value.getClass) => newT.set(key, value)
        // If it can't be assigned and the value is a map, it very likely that the field "key" of T is of some type U 
        // but has been deserialized to a Map[String, Object]. In this case we retrieve the type U from the field and recurse
        // to deserialize properly
        case (Some(f), map: java.util.Map[String, Object] @unchecked) if classOf[GenericJson].isAssignableFrom(f.getType) =>
          val deserializedInnerAttribute = deserializeTo(map)(ClassTag[GenericJson](f.getType))
          newT.set(key, deserializedInnerAttribute)
        // The set method trips up on some type mismatch between number types, this helps it
        case (Some(f), number: Number) if f.getType == Integer.TYPE => newT.set(key, number.intValue())
        case (Some(f), number: Number) if f.getType == classOf[Double] => newT.set(key, number.doubleValue())
        case (Some(f), number: Number) if f.getType == classOf[Float] => newT.set(key, number.floatValue())
        case (Some(f), number: Number) if f.getType == classOf[Long] => newT.set(key, number.longValue())
        // If either the key is not an attribute of T, or we can't assign it - just skip it
        // Throwing here would fail the response interpretation and eventually the workflow, which seems excessive
        // and would make this logic too fragile.
        // The only effect is that an attribute might not be populated and would be null.
        // We would only notice if we do look at this attribute though, which we only do with the purpose of populating metadata
        // Worst case scenario is thus "a metadata value is null" which seems better over failing the workflow
        case _ => 
      }
    }

    // Go over the map entries and use the "set" method of GenericJson to set the attributes.
    attributes.asScala.foreach((handleMap _).tupled)
    newT
  }
}
