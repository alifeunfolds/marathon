package mesosphere.marathon
package core.task.tracker.impl

import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.task.{ Task, TaskCondition }
import mesosphere.marathon.core.task.state.NetworkInfo
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.stream.Implicits._
import org.slf4j.LoggerFactory

/**
  * Converts between [[Task]] objects and their serialized representation MarathonTask.
  */
object TaskSerializer {

  private[this] val log = LoggerFactory.getLogger(getClass)

  def fromProto(proto: Protos.MarathonTask): Task = {

    def required[T](name: String, maybeValue: Option[T]): T = {
      maybeValue.getOrElse(throw new IllegalArgumentException(s"task[${proto.getId}]: $name must be set"))
    }

    def opt[T](
      hasAttribute: Protos.MarathonTask => Boolean, getAttribute: Protos.MarathonTask => T): Option[T] = {

      if (hasAttribute(proto)) {
        Some(getAttribute(proto))
      } else {
        None
      }
    }

    lazy val maybeAppVersion: Option[Timestamp] = opt(_.hasVersion, _.getVersion).map(Timestamp.apply)

    lazy val hostPorts = proto.getPortsList.map(_.intValue())(collection.breakOut)

    val mesosStatus = opt(_.hasStatus, _.getStatus)
    val hostName = required("host", opt(_.hasOBSOLETEHost, _.getOBSOLETEHost))
    val ipAddresses = mesosStatus.map(NetworkInfo.resolveIpAddresses).getOrElse(Nil)
    val networkInfo: NetworkInfo = NetworkInfo(hostName, hostPorts, ipAddresses)
    log.debug(s"Deserialized networkInfo: $networkInfo")

    val taskStatus = {
      Task.Status(
        stagedAt = Timestamp(proto.getStagedAt),
        startedAt = opt(_.hasStartedAt, _.getStartedAt).map(Timestamp.apply),
        mesosStatus = mesosStatus,
        condition = opt(
          // Invalid could also mean UNKNOWN since it's the default value of an enum
          t => t.hasCondition && t.getCondition != Protos.MarathonTask.Condition.Invalid,
          _.getCondition
        ).flatMap(TaskConditionSerializer.fromProto)
          // although this is an optional field, migration should have really taken care of this.
          // because of a bug in migration, some empties slipped through. so we make up for it here.
          .orElse(opt(_.hasStatus, _.getStatus).map(TaskCondition.apply))
          .getOrElse(Condition.Unknown),
        networkInfo = networkInfo
      )
    }

    constructTask(
      taskId = Task.Id(proto.getId),
      taskStatus,
      maybeAppVersion
    )
  }

  private[this] def constructTask(
    taskId: Task.Id,
    taskStatus: Task.Status,
    maybeVersion: Option[Timestamp]): Task = {

    val runSpecVersion = maybeVersion.getOrElse {
      // we cannot default to something meaningful here because Reserved tasks have no runSpec version
      // the version for a reserved task will however not be considered and when a new task is launched,
      // it will be given the latest runSpec version
      log.warn(s"$taskId has no version. Defaulting to Timestamp.zero")
      Timestamp.zero
    }

    Task(taskId, runSpecVersion, taskStatus)
  }

  def toProto(task: Task): Protos.MarathonTask = {
    val builder = Protos.MarathonTask.newBuilder()

    def setId(taskId: Task.Id): Unit = builder.setId(taskId.idString)
    def setLaunched(status: Task.Status, hostPorts: Seq[Int]): Unit = {
      builder.setStagedAt(status.stagedAt.millis)
      status.startedAt.foreach(startedAt => builder.setStartedAt(startedAt.millis))
      status.mesosStatus.foreach(status => builder.setStatus(status))
      builder.addAllPorts(hostPorts.map(Integer.valueOf).asJava)
    }
    def setVersion(appVersion: Timestamp): Unit = {
      builder.setVersion(appVersion.toString)
    }
    def setTaskCondition(condition: Condition): Unit = {
      builder.setCondition(TaskConditionSerializer.toProto(condition))
    }
    // this is needed for unit tests; need to be able to serialize deprecated fields and verify
    // they're deserialized correctly
    def setNetworkInfo(networkInfo: NetworkInfo): Unit = {
      builder.setOBSOLETEHost(networkInfo.hostName)
    }

    setId(task.taskId)
    setTaskCondition(task.status.condition)
    setNetworkInfo(task.status.networkInfo)
    setVersion(task.runSpecVersion)

    setLaunched(task.status, task.status.networkInfo.hostPorts)
    builder.build()
  }
}

object TaskConditionSerializer {

  import mesosphere._
  import mesosphere.marathon.core.condition.Condition._

  private val proto2model = Map(
    marathon.Protos.MarathonTask.Condition.Reserved -> Reserved,
    marathon.Protos.MarathonTask.Condition.Created -> Created,
    marathon.Protos.MarathonTask.Condition.Error -> Error,
    marathon.Protos.MarathonTask.Condition.Failed -> Failed,
    marathon.Protos.MarathonTask.Condition.Finished -> Finished,
    marathon.Protos.MarathonTask.Condition.Killed -> Killed,
    marathon.Protos.MarathonTask.Condition.Killing -> Killing,
    marathon.Protos.MarathonTask.Condition.Running -> Running,
    marathon.Protos.MarathonTask.Condition.Staging -> Staging,
    marathon.Protos.MarathonTask.Condition.Starting -> Starting,
    marathon.Protos.MarathonTask.Condition.Unreachable -> Unreachable,
    marathon.Protos.MarathonTask.Condition.Gone -> Gone,
    marathon.Protos.MarathonTask.Condition.Unknown -> Unknown,
    marathon.Protos.MarathonTask.Condition.Dropped -> Dropped
  )

  private val model2proto: Map[Condition, marathon.Protos.MarathonTask.Condition] =
    proto2model.map(_.swap)

  def fromProto(proto: Protos.MarathonTask.Condition): Option[Condition] = {
    proto2model.get(proto)
  }

  def toProto(taskCondition: Condition): Protos.MarathonTask.Condition = {
    model2proto.getOrElse(
      taskCondition,
      throw SerializationFailedException(s"Unable to serialize $taskCondition"))
  }
}
