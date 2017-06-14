import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.util
import java.util.function.IntBinaryOperator
import javax.imageio.ImageIO

import scala.collection.mutable

/**
  * Created by takirala on 6/13/17.
  */
class ImageState(val fileName: String) {

  val nBins = 100 //For precision.
  val pixelChunkSize = 200000 //Each chunk has this number of pixels

  private val _initialChunks = mutable.Queue[Array[Float]]()
  private val _intermediateMaps = mutable.Queue[mutable.Map[Int, Int]]()

  bootstrap()

  def isDone = _initialChunks.isEmpty && _intermediateMaps.isEmpty

  def hasWork = _initialChunks.nonEmpty || _intermediateMaps.nonEmpty

  def createRGBImage(bHist: Seq[(Int, Int)], hsbArray: Array[Array[Float]]): Unit = {
    val binFreq = Array.fill(nBins) {
      0
    }
    bHist.map(x => {
      binFreq(x._1) = x._2
    })
    util.Arrays.parallelPrefix(binFreq, new IntBinaryOperator {
      override def applyAsInt(left: Int, right: Int): Int = left + right
    })
    val cumProb = Array.fill(nBins)(0.0f)

    for (i <- 0 until nBins) {
      cumProb(i) = binFreq(i).toFloat / hsbArray.length
    }
    val newRgbArray = hsbArray.map(x => {
      val brightness = x(2)
      val binIndex = (brightness * (nBins - 1)).toInt
      Color.HSBtoRGB(x(0), x(1), cumProb(binIndex))
    })
    val image = ImageIO.read(new File(fileName))
    val newImage = new BufferedImage(image.getWidth(), image.getHeight(), image.getType)
    val output = new File(fileName + ".output.jpg")
    newImage.setRGB(0, 0, image.getWidth(), image.getHeight(), newRgbArray, 0, image.getWidth())
    ImageIO.write(newImage, "jpg", output)
  }

  private def bootstrap(): Unit = {
    val image = ImageIO.read(new File(fileName))
    val w = image.getWidth()
    val h = image.getHeight()
    val rgbArray = image.getRGB(0, 0, w, h, null, 0, w)
    val hsbArray = rgbArray.map(x => {
      val c = new Color(x)
      Color.RGBtoHSB(c.getRed, c.getGreen, c.getBlue, null)
    })

    for (chunk <- hsbArray.grouped(pixelChunkSize)) {
      val brightNessChunk = Array[Float]()
      for (hsbPixel <- chunk) {
        brightNessChunk :+ hsbPixel(2)
      }
      _initialChunks += brightNessChunk
    }
  }
}
