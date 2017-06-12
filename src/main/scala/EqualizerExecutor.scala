import com.google.protobuf.ByteString
import org.apache.mesos.Protos.{TaskState, TaskStatus}
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver, Protos}

/**
  * Created by takirala on 6/12/17.
  */
class EqualizerExecutor extends Executor{
  override def registered(driver: ExecutorDriver, executorInfo: Protos.ExecutorInfo, frameworkInfo: Protos.FrameworkInfo, slaveInfo: Protos.SlaveInfo): Unit = ???

  override def launchTask(driver: ExecutorDriver, task: Protos.TaskInfo): Unit = {
    println("Hello there, task has been launched.")
    driver.sendStatusUpdate(
      TaskStatus.newBuilder.setTaskId(task.getTaskId)
        .setState(TaskState.TASK_RUNNING)
        .build()
    )

    println("Cool computation here")

    driver.sendStatusUpdate(TaskStatus.newBuilder()
      .setTaskId(task.getTaskId)
      .setState(TaskState.TASK_FINISHED)
      .setData(ByteString.copyFrom("End of world".getBytes()))
      .build())
  }

  def main(args: Array[String]): Unit = {
    val driver = new MesosExecutorDriver(EqualizerExecutor.this)
    driver.run()
  }

  override def error(driver: ExecutorDriver, message: String): Unit = ???

  override def killTask(driver: ExecutorDriver, taskId: Protos.TaskID): Unit = ???

  override def disconnected(driver: ExecutorDriver): Unit = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: Protos.SlaveInfo): Unit = ???

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = ???

  override def shutdown(driver: ExecutorDriver): Unit = ???
}
