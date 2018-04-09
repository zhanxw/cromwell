package cromwell.backend.google.pipelines.v2alpha1

import com.google.api.services.genomics.v2alpha1.model.{Disk, Mount}
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.common.io.PipelinesApiAttachedDisk
import cromwell.backend.google.pipelines.common.{PipelinesApiFileInput, PipelinesApiFileOutput, PipelinesApiLiteralInput, PipelinesApiRuntimeAttributes}
import cromwell.backend.google.pipelines.v2alpha1.api.ActionBuilder._
import cromwell.backend.google.pipelines.v2alpha1.api.ActionFlag
import wdl4s.parser.MemoryUnit

object PipelinesConversions {
  implicit class DiskConversion(val disk: PipelinesApiAttachedDisk) extends AnyVal {
    def toMount = new Mount()
      .setDisk(disk.name)
      .setPath(disk.mountPoint.pathAsString)

    def toDisk = new Disk()
      .setName(disk.name)
      .setSizeGb(disk.sizeGb)
      .setType(disk.diskType.googleTypeName)
  }

  implicit class EnhancedCreatePipelineParameters(val parameters: CreatePipelineParameters) extends AnyVal {
    def toMounts: List[Mount] = parameters.runtimeAttributes.disks.map(_.toMount).toList
    def toDisks: List[Disk] = parameters.runtimeAttributes.disks.map(_.toDisk).toList
  }

  implicit class EnhancedFileInput(val fileInput: PipelinesApiFileInput) extends AnyVal {
    def toEnvironment = Map(fileInput.name -> fileInput.containerPath)

    def toAction(mounts: List[Mount]) = gsutil("cp", fileInput.cloudPath, fileInput.containerPath)(mounts)

    def toMount = {
      new Mount()
        .setDisk(fileInput.mount.name)
        .setPath(fileInput.mount.mountPoint.pathAsString)
    }
  }

  implicit class EnhancedFileOutput(val fileOutput: PipelinesApiFileOutput) extends AnyVal {
    def toEnvironment = Map(fileOutput.name -> fileOutput.containerPath)

    def toAction(mounts: List[Mount], gsutilFlags: List[String] = List.empty) = {
      gsutil("cp", fileOutput.containerPath, fileOutput.cloudPath)(mounts, List(ActionFlag.AlwaysRun))
    }

    def toMount = {
      new Mount()
        .setDisk(fileOutput.mount.name)
        .setPath(fileOutput.mount.mountPoint.pathAsString)
    }
  }

  implicit class EnhancedinputLiteral(val literalInput: PipelinesApiLiteralInput) extends AnyVal {
    def toEnvironment = Map(literalInput.name -> literalInput.value)
  }

  implicit class EnhancedAttributes(val attributes: PipelinesApiRuntimeAttributes) extends AnyVal {
    def toMachineType = {
      val cpu = attributes.cpu
      val memory = attributes.memory.to(MemoryUnit.MB).asMultipleOf(256).amount.toInt
      s"custom-$cpu-$memory"
    }
  }
}
