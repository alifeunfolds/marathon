package mesosphere.marathon
package core.task.tracker.impl

import mesosphere.UnitTest
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.LocalVolumeId
import mesosphere.marathon.core.instance.TestTaskBuilder
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.state.NetworkInfo
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.stream.Implicits._
import mesosphere.marathon.test.MarathonTestHelper
import org.apache.mesos.{ Protos => MesosProtos }

class TaskSerializerTest extends UnitTest {

  "TaskSerializer" should {
    "minimal marathonTask => Task" in {
      val f = new Fixture
      Given("a minimal MarathonTask")
      val now = MarathonTestHelper.clock.now()
      val taskProto = MarathonTask.newBuilder()
        .setId("task")
        .setVersion(now.toString)
        .setStagedAt(now.millis)
        .setCondition(MarathonTask.Condition.Running)
        .setOBSOLETEHost(f.sampleHost)
        .build()

      When("we convert it to task")
      val task = TaskSerializer.fromProto(taskProto)

      Then("we get a minimal task State")
      val expectedState = TestTaskBuilder.Helper.minimalTask(f.taskId, now, None, Condition.Running)

      task should be(expectedState)

      When("we serialize it again")
      val marathonTask2 = TaskSerializer.toProto(task)

      Then("we get the original state back")
      marathonTask2 should equal(taskProto)
    }

    "full marathonTask with no networking => Task" in {
      val f = new Fixture

      Given("a MarathonTask with all fields and host ports")
      val taskProto = f.completeTask

      When("we convert it to task")
      val task = TaskSerializer.fromProto(taskProto)

      Then("we get the expected task state")
      val expectedState = f.fullSampleTaskStateWithoutNetworking

      task should be(expectedState)

      When("we serialize it again")
      val marathonTask2 = TaskSerializer.toProto(task)

      Then("we get the original state back")
      marathonTask2 should equal(taskProto)
    }

    "full marathonTask with host ports => Task" in {
      val f = new Fixture

      Given("a MarathonTask with all fields and host ports")
      val samplePorts = Seq(80, 81)
      val taskProto =
        f.completeTask.toBuilder
          .addAllPorts(samplePorts.map(Integer.valueOf(_)).asJava)
          .build()

      When("we convert it to task")
      val task = TaskSerializer.fromProto(taskProto)

      Then("we get the expected task state")
      val expectedState = f.fullSampleTaskStateWithoutNetworking.copy(
        status = f.fullSampleTaskStateWithoutNetworking.status.copy(
          networkInfo = f.fullSampleTaskStateWithoutNetworking.status.networkInfo.copy(hostPorts = samplePorts)
        )
      )

      task should be(expectedState)

      When("we serialize it again")
      val marathonTask2 = TaskSerializer.toProto(task)

      Then("we get the original state back")
      marathonTask2 should equal(taskProto)
    }

    "full marathonTask with NetworkInfoList in Status => Task" in {
      val f = new Fixture

      Given("a MarathonTask with all fields and status with network infos")
      val taskProto =
        f.completeTask.toBuilder
          .setStatus(
            MesosProtos.TaskStatus.newBuilder()
              .setTaskId(f.taskId.mesosTaskId)
              .setState(MesosProtos.TaskState.TASK_RUNNING)
              .setContainerStatus(MesosProtos.ContainerStatus.newBuilder().addAllNetworkInfos(f.sampleNetworks.asJava))
          )
          .build()

      When("we convert it to task")
      val task = TaskSerializer.fromProto(taskProto)

      Then("we get the expected task state")
      import MarathonTestHelper.Implicits._
      val expectedState = f.fullSampleTaskStateWithoutNetworking.withNetworkInfo(networkInfos = f.sampleNetworks)

      task should be(expectedState)

      When("we serialize it again")
      val marathonTask2 = TaskSerializer.toProto(task)

      Then("we get the original state back")
      marathonTask2 should equal(taskProto)
    }

  }

  class Fixture {
    private[this] val appId = PathId.fromSafePath("/test")
    val taskId = Task.Id.forRunSpec(appId)
    val sampleHost: String = "host.some"
    private[this] val sampleAttributes: Seq[MesosProtos.Attribute] = Seq(attribute("label1", "value1"))
    private[this] val stagedAtLong: Long = 1
    private[this] val startedAtLong: Long = 2
    private[this] val appVersion: Timestamp = Timestamp(3)
    private[this] val sampleTaskStatus: MesosProtos.TaskStatus =
      MesosProtos.TaskStatus.newBuilder()
        .setTaskId(MesosProtos.TaskID.newBuilder().setValue(taskId.idString))
        .setState(MesosProtos.TaskState.TASK_RUNNING)
        .build()
    private[this] val sampleSlaveId: MesosProtos.SlaveID.Builder = MesosProtos.SlaveID.newBuilder().setValue("slaveId")
    val sampleNetworks: Seq[MesosProtos.NetworkInfo] =
      Seq(
        MesosProtos.NetworkInfo.newBuilder()
          .addIpAddresses(MesosProtos.NetworkInfo.IPAddress.newBuilder().setIpAddress("1.2.3.4"))
          .build()
      )
    val fullSampleTaskStateWithoutNetworking: Task =
      Task(
        taskId,
        runSpecVersion = appVersion,
        status = Task.Status(
          stagedAt = Timestamp(stagedAtLong),
          startedAt = Some(Timestamp(startedAtLong)),
          mesosStatus = Some(sampleTaskStatus),
          condition = Condition.Running,
          networkInfo = NetworkInfo(sampleHost, hostPorts = Nil, ipAddresses = Nil)
        )
      )

    val completeTask =
      MarathonTask
        .newBuilder()
        .setId(taskId.idString)
        .setStagedAt(stagedAtLong)
        .setStartedAt(startedAtLong)
        .setVersion(appVersion.toString)
        .setStatus(sampleTaskStatus)
        .setCondition(MarathonTask.Condition.Running)
        .setReservation(MarathonTask.Reservation.newBuilder
          .addLocalVolumeIds(LocalVolumeId(appId, "my-volume", "uuid-123").idString)
          .setState(MarathonTask.Reservation.State.newBuilder()
            .setType(MarathonTask.Reservation.State.Type.Launched)))
        .setOBSOLETEHost(sampleHost)
        .build()

    private[this] def attribute(name: String, textValue: String): MesosProtos.Attribute = {
      val text = MesosProtos.Value.Text.newBuilder().setValue(textValue)
      MesosProtos.Attribute.newBuilder().setName(name).setType(MesosProtos.Value.Type.TEXT).setText(text).build()
    }

    object Resident {
      import scala.concurrent.duration._

      private[this] val appId = PathId("/test")
      private[this] val taskId = Task.Id("reserved1")
      private[this] val now = MarathonTestHelper.clock.now()
      private[this] val containerPath = "containerPath"
      private[this] val uuid = "uuid"
      private[this] val stagedAt = now - 1.minute
      private[this] val startedAt = now - 55.seconds
      private[this] val mesosStatus = TestTaskBuilder.Helper.statusForState(taskId.idString, MesosProtos.TaskState.TASK_RUNNING)
      private[this] val hostPorts = Seq(1, 2, 3)
    }
  }
}
