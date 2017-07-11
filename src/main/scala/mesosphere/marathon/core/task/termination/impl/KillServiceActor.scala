package mesosphere.marathon
package core.task.termination.impl

import akka.Done
import akka.actor.{ Actor, Cancellable, Props }
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.event.{ InstanceChanged, UnknownInstanceTerminated }
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.Task.Id
import mesosphere.marathon.core.task.termination.InstanceChangedPredicates.considerTerminal
import mesosphere.marathon.core.task.termination.KillConfig
import mesosphere.marathon.core.task.tracker.TaskStateOpProcessor
import mesosphere.marathon.state.Timestamp

import scala.collection.mutable
import scala.concurrent.{ Future, Promise }

/**
  * An actor that handles killing instances in chunks and depending on the instance state.
  * Lost instances will simply be expunged from state, while active instances will be killed
  * via the scheduler driver. There is be a maximum number of kills in flight, and
  * the service will only issue more kills when instances are reported terminal.
  *
  * If a kill is not acknowledged with a terminal status update within a configurable
  * time window, the kill is retried a configurable number of times. If the maximum
  * number of retries is exceeded, the instance will be expunged from state similar to a
  * lost instance.
  *
  * For pods started via the default executor, it is sufficient to kill 1 task of the group,
  * which will cause all tasks to be killed
  *
  * See [[KillConfig]] for configuration options.
  */
private[impl] class KillServiceActor(
    driverHolder: MarathonSchedulerDriverHolder,
    stateOpProcessor: TaskStateOpProcessor,
    config: KillConfig,
    clock: Clock) extends Actor with StrictLogging {
  import KillServiceActor._
  import context.dispatcher

  val instancesToKill: mutable.HashMap[Instance.Id, ToKill] = mutable.HashMap.empty
  val inFlight: mutable.HashMap[Instance.Id, ToKill] = mutable.HashMap.empty

  // We instantiate the materializer here so that all materialized streams end up as children of this actor
  implicit val materializer = ActorMaterializer()

  val retryTimer: RetryTimer = new RetryTimer {
    override def createTimer(): Cancellable = {
      context.system.scheduler.schedule(config.killRetryTimeout, config.killRetryTimeout, self, Retry)
    }
  }

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[InstanceChanged])
    context.system.eventStream.subscribe(self, classOf[UnknownInstanceTerminated])
  }

  override def postStop(): Unit = {
    retryTimer.cancel()
    context.system.eventStream.unsubscribe(self)
    if (instancesToKill.nonEmpty) {
      logger.warn(s"Stopping $self, but not all tasks have been killed. Remaining: ${instancesToKill.keySet.mkString(", ")}, inFlight: ${inFlight.keySet.mkString(", ")}")
    }
  }

  override def receive: Receive = {
    case KillUnknownTaskById(taskId) =>
      killUnknownTaskById(taskId)

    case KillInstances(instances, promise) =>
      killInstances(instances, promise)

    case InstanceChanged(id, _, _, condition, _) if considerTerminal(condition) &&
      (inFlight.contains(id) || instancesToKill.contains(id)) =>
      handleTerminal(id)

    case UnknownInstanceTerminated(id, _, _) if inFlight.contains(id) || instancesToKill.contains(id) =>
      handleTerminal(id)

    case Retry =>
      retry()
  }

  def killUnknownTaskById(taskId: Task.Id): Unit = {
    logger.debug(s"Received KillUnknownTaskById($taskId)")
    val promise = Promise[Done]
    instancesToKill.update(taskId.instanceId, ToKill(taskId.instanceId, Seq(taskId), maybeInstance = None, attempts = 0, promise = promise))
    processKills()
  }

  def killInstances(instances: Seq[Instance], promise: Promise[Done]): Unit = {
    if (instances.isEmpty) promise.trySuccess(Done)

    val instanceIds = instances.map(_.instanceId)
    logger.debug(s"Adding instances $instanceIds to queue")

    val instanceKilledFutures = Seq.newBuilder[Future[Done]]

    instances.foreach { instance =>

      // This promise is completed once this instance has been killed.
      val killPromise = Promise[Done]()
      instanceKilledFutures += killPromise.future

      // TODO(PODS): do we make sure somewhere that an instance has _at_least_ one task?
      val taskIds: IndexedSeq[Id] = instance.tasksMap.values.withFilter(!_.isTerminal).map(_.taskId)(collection.breakOut)
      instancesToKill.update(
        instance.instanceId,
        ToKill(instance.instanceId, taskIds, maybeInstance = Some(instance), attempts = 0, killPromise)
      )
    }

    // Complete promise once all instances have been killed.
    val allInstancesKilled: Future[Done] = Future.sequence(instanceKilledFutures.result()).map(_ => Done)
    promise.completeWith(allInstancesKilled)

    processKills()
  }

  def processKills(): Unit = {
    val killCount = config.killChunkSize - inFlight.size
    val toKillNow = instancesToKill.take(killCount)

    logger.info(s"processing ${toKillNow.size} kills for ${toKillNow.keys}")
    toKillNow.foreach {
      case (instanceId, data) => processKill(data)
    }

    if (inFlight.isEmpty) {
      retryTimer.cancel()
    } else {
      retryTimer.setup()
    }
  }

  def processKill(toKill: ToKill): Unit = {

    KillAction(toKill.instanceId, toKill.taskIdsToKill, toKill.maybeInstance) match {
      case KillAction.Noop =>
        ()

      case KillAction.IssueKillRequest =>
        driverHolder.driver.foreach { driver =>
          toKill.taskIdsToKill.map(_.mesosTaskId).foreach(driver.killTask)
        }
        val attempts = toKill.attempts + 1
        inFlight.update(
          toKill.instanceId, toKill.copy(attempts = attempts, issued = clock.now())
        )

      case KillAction.ExpungeFromState =>
        stateOpProcessor.process(InstanceUpdateOperation.ForceExpunge(toKill.instanceId))
        // TODO: When should the promise be fulfilled?
        toKill.promise.trySuccess(Done)
    }

    instancesToKill.remove(toKill.instanceId)
  }

  def handleTerminal(instanceId: Instance.Id): Unit = {
    instancesToKill.remove(instanceId)
    inFlight.remove(instanceId).map(_.promise.trySuccess(Done))
    logger.debug(s"$instanceId is terminal. (${instancesToKill.size} kills queued, ${inFlight.size} in flight)")
    processKills()
  }

  def retry(): Unit = {
    val now = clock.now()

    inFlight.foreach {
      case (instanceId, toKill) if (toKill.issued + config.killRetryTimeout) < now =>
        logger.warn(s"No kill ack received for $instanceId, retrying (${toKill.attempts} attempts so far)")
        processKill(toKill)

      case _ => // ignore
    }
  }
}

private[termination] object KillServiceActor {

  sealed trait Request extends InternalRequest
  case class KillInstances(instances: Seq[Instance], promise: Promise[Done]) extends Request
  case class KillUnknownTaskById(taskId: Task.Id) extends Request

  sealed trait InternalRequest
  case object Retry extends InternalRequest

  def props(
    driverHolder: MarathonSchedulerDriverHolder,
    stateOpProcessor: TaskStateOpProcessor,
    config: KillConfig,
    clock: Clock): Props = Props(
    new KillServiceActor(driverHolder, stateOpProcessor, config, clock))

  /**
    * Metadata used to track which instances to kill and how many attempts have been made
    *
    * @param instanceId id of the instance to kill
    * @param taskIdsToKill ids of the tasks to kill
    * @param maybeInstance the instance, if available
    * @param attempts the number of kill attempts
    * @param promise Promise that is fulfilled once task has been killed
    * @param issued the time of the last issued kill request
    */
  case class ToKill(
    instanceId: Instance.Id,
    taskIdsToKill: Seq[Task.Id],
    maybeInstance: Option[Instance],
    attempts: Int,
    promise: Promise[Done],
    issued: Timestamp = Timestamp.zero)
}

/**
  * Wraps a timer into an interface that hides internal mutable state behind simple setup and cancel methods
  */
private[this] trait RetryTimer {
  private[this] var retryTimer: Option[Cancellable] = None

  /** Creates a new timer when setup() is called */
  def createTimer(): Cancellable

  /**
    * Cancel the timer if there is one.
    */
  final def cancel(): Unit = {
    retryTimer.foreach(_.cancel())
    retryTimer = None
  }

  /**
    * Setup a timer if there is no timer setup already. Will do nothing if there is a timer.
    * Note that if the timer is scheduled only once, it will not be removed until you call cancel.
    */
  final def setup(): Unit = {
    if (retryTimer.isEmpty) {
      retryTimer = Some(createTimer())
    }
  }
}
