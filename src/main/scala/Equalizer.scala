import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos._

/**
  * Created by takirala on 6/12/17.
  */
object Equalizer {

  def main(args: Array[String]): Unit = {

    // String constants
    val name = "ImageEqualizer"
    val mesosMaster = "35.160.165.209"

    val id = FrameworkID.newBuilder.setValue(name).build()

    val executorCommand = CommandInfo.newBuilder().setValue("echo task ran successfully").build()

    val executorId = ExecutorID.newBuilder.setValue("EqualizerExecutor-"+System.currentTimeMillis())

    val executor = ExecutorInfo.newBuilder()
      .setCommand(executorCommand)
      .setExecutorId(executorId)
      .setName(name)
      .build()

    val scheduler = new EqualizerScheduler()

    val framework = FrameworkInfo.newBuilder
      .setName(name)
        .setUser("")
      .build()

    val driver = new MesosSchedulerDriver(scheduler, framework, mesosMaster)
    driver.run()
  }

}