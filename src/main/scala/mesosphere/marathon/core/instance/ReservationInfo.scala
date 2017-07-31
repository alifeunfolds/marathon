package mesosphere.marathon
package core.instance

import com.fasterxml.uuid.{ EthernetAddress, Generators }
import mesosphere.marathon.state.{ PathId, PersistentVolume, Timestamp }
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.core.instance.ReservationInfo.Timeout.Reason
import play.api.libs.functional.syntax._
import play.api.libs.json._

case class ReservationInfo(volumeIds: Seq[LocalVolumeId], labels: Map[String, String], timeout: Option[ReservationInfo.Timeout])

object ReservationInfo {

  val Empty: Option[ReservationInfo] = Option.empty[ReservationInfo]

  /**
    * A timeout that eventually leads to a state transition
    *
    * @param initiated When this timeout was setup
    * @param deadline When this timeout should become effective
    * @param reason The reason why this timeout was set up
    */
  case class Timeout(initiated: Timestamp, deadline: Timestamp, reason: Timeout.Reason)

  object Timeout {
    sealed trait Reason
    object Reason {
      /** A timeout because the task could not be relaunched */
      case object RelaunchEscalationTimeout extends Reason
      /** A timeout because we got no ack for reserved resources or persistent volumes */
      case object ReservationTimeout extends Reason
    }

    val Empty: Option[Timeout] = Option.empty[Timeout]
  }

  // I'm honestly sorry for this formats mess, this needs to be cleaned up -- copy & paste so far
  implicit object ReasonFormat extends Format[Timeout.Reason] {
    override def reads(json: JsValue): JsResult[Timeout.Reason] = {
      json.validate[String].map {
        case "RelaunchEscalationTimeout" => Reason.RelaunchEscalationTimeout
        case "ReservationTimeout" => Reason.ReservationTimeout
      }
    }

    override def writes(o: Timeout.Reason): JsValue = {
      JsString(o.toString)
    }
  }

  implicit val timeoutFormat = Json.format[Timeout]

  implicit val localVolumeIdReader = (
    (__ \ "runSpecId").read[PathId] and
    (__ \ "containerPath").read[String] and
    (__ \ "uuid").read[String]
  )((id, path, uuid) => LocalVolumeId(id, path, uuid))

  implicit val localVolumeIdWriter = Writes[LocalVolumeId] { localVolumeId =>
    JsObject(Seq(
      "runSpecId" -> Json.toJson(localVolumeId.runSpecId),
      "containerPath" -> Json.toJson(localVolumeId.containerPath),
      "uuid" -> Json.toJson(localVolumeId.uuid),
      "persistenceId" -> Json.toJson(localVolumeId.idString)
    ))
  }

  implicit val reservationInfoFormat: Format[ReservationInfo] = Json.format[ReservationInfo]
}

case class LocalVolume(id: LocalVolumeId, persistentVolume: PersistentVolume)

case class LocalVolumeId(runSpecId: PathId, containerPath: String, uuid: String) {
  import LocalVolumeId._
  lazy val idString = runSpecId.safePath + delimiter + containerPath + delimiter + uuid

  override def toString: String = s"LocalVolume [$idString]"
}

object LocalVolumeId {
  private val uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface())
  private val delimiter = "#"
  private val LocalVolumeEncoderRE = s"^([^$delimiter]+)[$delimiter]([^$delimiter]+)[$delimiter]([^$delimiter]+)$$".r

  def apply(runSpecId: PathId, volume: PersistentVolume): LocalVolumeId =
    LocalVolumeId(runSpecId, volume.containerPath, uuidGenerator.generate().toString)

  def unapply(id: String): Option[(LocalVolumeId)] = id match {
    case LocalVolumeEncoderRE(runSpec, path, uuid) => Some(LocalVolumeId(PathId.fromSafePath(runSpec), path, uuid))
    case _ => None
  }
}
