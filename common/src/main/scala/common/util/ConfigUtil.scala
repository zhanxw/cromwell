package common.util

import com.typesafe.config.{Config, ConfigException}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Interval
import eu.timepit.refined.{W, refineV}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

object ConfigUtil {
  type OneToOneHundred = Interval.Open[W.`1`.T, W.`100`.T]
  type PositivePercentage = Int Refined OneToOneHundred

  implicit val percentageReader = new ValueReader[PositivePercentage] {
    override def read(config: Config, path: String) = refineV[OneToOneHundred](config.as[Int](path)) match {
      case Left(_) => throw new ConfigException.BadValue(path, s"Value ${config.as[String](path)} is not a positive integer")
      case Right(refined) => refined
    }
  }
}
