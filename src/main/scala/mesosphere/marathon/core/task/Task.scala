package mesosphere.marathon
package core.task

import java.util.Base64

import com.fasterxml.uuid.{ EthernetAddress, Generators }
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.condition.Condition.Terminal
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.pod.MesosContainer
import mesosphere.marathon.core.task.Task.{ Status, updatedHealthOrState }
import mesosphere.marathon.core.task.state.NetworkInfo
import mesosphere.marathon.core.task.update.{ TaskUpdateEffect, TaskUpdateOperation }
import mesosphere.marathon.state._
import org.apache.mesos
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.Protos.{ TaskState, TaskStatus }
import org.apache.mesos.{ Protos => MesosProtos }
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
// TODO PODS remove api imports
import mesosphere.marathon.api.v2.json.Formats._
import play.api.libs.json._

/**
  * <pre>
  *                            _  _
  *   | match offer      ___ (~ )( ~)
  *   | & launch        /   \_\ \/ /
  *   |                |   D_ ]\ \/
  *   |            +-> |   D _]/\ \
  *   |   terminal |    \___/ / /\ \
  *   |     status |         (_ )( _)
  *   v     update |       DELETED
  *                |
  * +--------------+--------+
  * | LaunchedEphemeral     |
  * |                       |
  * |  Started / Staged     |
  * |  Running              |
  * |                       |
  * +-----------------------+
  * </pre>
  */
case class Task(
    taskId: Task.Id,
    runSpecVersion: Timestamp,
    status: Status) {

  import Task.log

  private[this] def hasStartedRunning: Boolean = status.startedAt.isDefined

  val runSpecId: PathId = taskId.runSpecId

  val launchedMesosId: Option[MesosProtos.TaskID] = if (status.condition.isActive) {
    // it doesn't make sense for an inactive task
    Some(taskId.mesosTaskId)
  } else {
    None
  }

  /** apply the given operation to a task */
  def update(op: TaskUpdateOperation): TaskUpdateEffect = op match {
    // exceptional case: the task is already terminal. Don't transition in this case.
    // This might be because the task terminated (e.g. finished) before Marathon issues a kill Request
    // to Mesos. Mesos will likely send back a TASK_LOST status update, because the task is no longer
    // known in Mesos. We'll never want to transition from one terminal state to another as a terminal
    // state should already be distinct enough.
    // related to https://github.com/mesosphere/marathon/pull/4531
    case op: TaskUpdateOperation if this.isTerminal =>
      log.warn(s"received $op for terminal $taskId, ignoring")
      TaskUpdateEffect.Noop

    case TaskUpdateOperation.MesosUpdate(Condition.Running, mesosStatus, now) if !hasStartedRunning =>
      val updatedNetworkInfo = status.networkInfo.update(mesosStatus)
      val updatedTask = copy(status = status.copy(
        mesosStatus = Some(mesosStatus),
        condition = Condition.Running,
        startedAt = Some(now),
        networkInfo = updatedNetworkInfo
      ))
      TaskUpdateEffect.Update(newState = updatedTask)

    // The Terminal extractor applies specific logic e.g. when an Unreachable task becomes Gone
    case TaskUpdateOperation.MesosUpdate(newStatus: Terminal, mesosStatus, _) =>
      val updated = copy(status = status.copy(
        mesosStatus = Some(mesosStatus),
        condition = newStatus))
      TaskUpdateEffect.Update(updated)

    case TaskUpdateOperation.MesosUpdate(newStatus, mesosStatus, _) =>
      // TODO(PODS): strange to use Condition here
      updatedHealthOrState(status.mesosStatus, mesosStatus).map { newTaskStatus =>
        val updatedTask = copy(status = status.copy(
          mesosStatus = Some(newTaskStatus),
          condition = newStatus
        ))
        // TODO(PODS): The instance needs to handle a terminal task via an Update here
        // Or should we use Expunge in case of a terminal update for resident tasks?
        TaskUpdateEffect.Update(newState = updatedTask)
      } getOrElse {
        log.debug("Ignoring status update for {}. Status did not change.", taskId)
        TaskUpdateEffect.Noop
      }
  }

  /**
    * @return whether task has an unreachable Mesos status longer than timeout.
    */
  def isUnreachableExpired(now: Timestamp, timeout: FiniteDuration): Boolean = {
    if (status.condition == Condition.Unreachable || status.condition == Condition.UnreachableInactive) {
      status.mesosStatus.exists { status =>
        val since: Timestamp =
          if (status.hasUnreachableTime) status.getUnreachableTime
          else Timestamp.fromTaskStatus(status)

        since.expired(now, by = timeout)
      }
    } else false
  }

}

object Task {

  case class Id(idString: String) extends Ordered[Id] {
    lazy val mesosTaskId: MesosProtos.TaskID = MesosProtos.TaskID.newBuilder().setValue(idString).build()
    lazy val runSpecId: PathId = Id.runSpecId(idString)
    lazy val instanceId: Instance.Id = Id.instanceId(idString)
    lazy val containerName: Option[String] = Id.containerName(idString)
    override def toString: String = s"task [$idString]"
    override def compare(that: Id): Int = idString.compare(that.idString)
  }

  object Id {

    @SuppressWarnings(Array("LooksLikeInterpolatedString"))
    object Names {
      val anonymousContainer = "$anon" // presence of `$` is important since it's illegal for a real container name!
    }
    // Regular expression for matching taskIds before instance-era
    private val LegacyTaskIdRegex = """^(.+)[\._]([^_\.]+)$""".r

    // Regular expression for matching taskIds since instance-era
    private val TaskIdWithInstanceIdRegex = """^(.+)\.(instance-|marathon-)([^_\.]+)[\._]([^_\.]+)$""".r

    private val uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface())

    def runSpecId(taskId: String): PathId = {
      taskId match {
        case TaskIdWithInstanceIdRegex(runSpecId, prefix, instanceId, maybeContainer) => PathId.fromSafePath(runSpecId)
        case LegacyTaskIdRegex(runSpecId, uuid) => PathId.fromSafePath(runSpecId)
        case _ => throw new MatchError(s"taskId $taskId is no valid identifier")
      }
    }

    def containerName(taskId: String): Option[String] = {
      taskId match {
        case TaskIdWithInstanceIdRegex(runSpecId, prefix, instanceUuid, maybeContainer) =>
          if (maybeContainer == Names.anonymousContainer) None else Some(maybeContainer)
        case LegacyTaskIdRegex(runSpecId, uuid) => None
        case _ => throw new MatchError(s"taskId $taskId is no valid identifier")
      }
    }

    def instanceId(taskId: String): Instance.Id = {
      taskId match {
        case TaskIdWithInstanceIdRegex(runSpecId, prefix, instanceUuid, uuid) =>
          Instance.Id(runSpecId + "." + prefix + instanceUuid)
        case LegacyTaskIdRegex(runSpecId, uuid) =>
          Instance.Id(runSpecId + "." + calculateLegacyExecutorId(uuid))
        case _ => throw new MatchError(s"taskId $taskId is no valid identifier")
      }
    }

    def apply(mesosTaskId: MesosProtos.TaskID): Id = new Id(mesosTaskId.getValue)

    def forRunSpec(id: PathId): Id = {
      val taskId = id.safePath + "." + uuidGenerator.generate()
      Task.Id(taskId)
    }

    def forInstanceId(instanceId: Instance.Id, container: Option[MesosContainer]): Id =
      Id(instanceId.idString + "." + container.map(c => c.name).getOrElse(Names.anonymousContainer))

    implicit val taskIdFormat = Format(
      Reads.of[String](Reads.minLength[String](3)).map(Task.Id(_)),
      Writes[Task.Id] { id => JsString(id.idString) }
    )

    // pre-instance-era executorId="marathon-$taskId" and compatibility reasons we need this calculation.
    // Should be removed as soon as no tasks without instance exists (tbd)
    def calculateLegacyExecutorId(taskId: String): String = s"marathon-$taskId"
  }

  /**
    * Contains information about the status of a launched task including timestamps for important
    * state transitions.
    *
    * @param stagedAt Despite its name, stagedAt is set on task creation and before the TASK_STAGED notification from
    *                 Mesos. This is important because we periodically check for any tasks with an old stagedAt
    *                 timestamp and kill them (See KillOverdueTasksActor).
    */
  case class Status(
      stagedAt: Timestamp,
      startedAt: Option[Timestamp] = None,
      mesosStatus: Option[MesosProtos.TaskStatus] = None,
      condition: Condition,
      networkInfo: NetworkInfo) {

    /**
      * @return the health status reported by mesos for this task
      */
    def healthy: Option[Boolean] = mesosStatus.withFilter(_.hasHealthy).map(_.getHealthy)
  }

  object Status {
    implicit object MesosTaskStatusFormat extends Format[mesos.Protos.TaskStatus] {
      override def reads(json: JsValue): JsResult[mesos.Protos.TaskStatus] = {
        json.validate[String].map { base64 =>
          mesos.Protos.TaskStatus.parseFrom(Base64.getDecoder.decode(base64))
        }
      }

      override def writes(o: TaskStatus): JsValue = {
        JsString(Base64.getEncoder.encodeToString(o.toByteArray))
      }
    }
    implicit val statusFormat = Json.format[Status]
  }

  object Terminated {
    def isTerminated(state: TaskState): Boolean = state match {
      case TASK_ERROR | TASK_FAILED | TASK_FINISHED | TASK_KILLED | TASK_LOST => true
      case _ => false
    }

    def unapply(state: TaskState): Option[TaskState] = if (isTerminated(state)) Some(state) else None
  }

  /** returns the new status if the health status has been added or changed, or if the state changed */
  private[task] def updatedHealthOrState(
    maybeCurrent: Option[MesosProtos.TaskStatus],
    update: MesosProtos.TaskStatus): Option[MesosProtos.TaskStatus] = {

    maybeCurrent match {
      case Some(current) =>
        val healthy = update.hasHealthy && (!current.hasHealthy || current.getHealthy != update.getHealthy)
        val changed = healthy || current.getState != update.getState
        if (changed) {
          Some(update)
        } else {
          None
        }
      case None => Some(update)
    }
  }

  implicit class TaskStatusComparison(val task: Task) extends AnyVal {
    // TODO: a task cannot be in reserved anymore, as the reservation info has moved to instance level
    def isReserved: Boolean = task.status.condition == Condition.Reserved
    def isCreated: Boolean = task.status.condition == Condition.Created
    def isError: Boolean = task.status.condition == Condition.Error
    def isFailed: Boolean = task.status.condition == Condition.Failed
    def isFinished: Boolean = task.status.condition == Condition.Finished
    def isKilled: Boolean = task.status.condition == Condition.Killed
    def isKilling: Boolean = task.status.condition == Condition.Killing
    def isRunning: Boolean = task.status.condition == Condition.Running
    def isStaging: Boolean = task.status.condition == Condition.Staging
    def isStarting: Boolean = task.status.condition == Condition.Starting
    def isUnreachable: Boolean = task.status.condition == Condition.Unreachable
    def isUnreachableInactive: Boolean = task.status.condition == Condition.UnreachableInactive
    def isGone: Boolean = task.status.condition == Condition.Gone
    def isUnknown: Boolean = task.status.condition == Condition.Unknown
    def isDropped: Boolean = task.status.condition == Condition.Dropped
    def isTerminal: Boolean = task.status.condition.isTerminal
    def isActive: Boolean = task.status.condition.isActive
  }

  private[task] val log = LoggerFactory.getLogger(getClass)
  implicit val taskFormat = Json.format[Task]
}
