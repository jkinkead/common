package org.allenai.common

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.NoSuchElementException

import scala.io.Codec

/** A class that combines the best elements of java.nio.channels.FileChannel and scala.io.Source.
  * This provides:
  * <ul>
  * <li>byte-valued position in the file
  * <li>seekability (seek-to-position)
  * <li>character iteration
  * <li>optional memory-mapping of files for fast reading
  * </ul>
  * Memory-mapped files will be read in chunks of one gigabyte. Memory mapping is only recommended
  * for larger files, and may hurt performance for small files. It may also hurt performance for
  * files larger than a gigabyte that are repositioned frequently, as calling `position(Long)` will
  * result in a remap if the new position is in a different gigabyte segment.
  * @param inFile the file channel to wrap
  * @param bufferSize the size of the internal buffer to use
  * @param memoryMap if true, map the file into memory
  * @param codec the codec to use. Must be one of UTF-8 or ISO-8859-1.
  */
class ChannelSource(
    inFile: FileChannel,
    memoryMap: Boolean = false,
    bufferSize: Int = 1024
)(implicit codec: Codec) extends Iterator[Char] {
  require(bufferSize >= 3, "Buffer must be at least 3 bytes to decode UTF-8!")

  /** True if we're decoding as UTF-8; false if we're decoding as ISO-8859-1. */
  val useUtf8 = if (codec.name == Codec.UTF8.name) {
    true
  } else if (codec.name == Codec.ISO8859.name) {
    false
  } else {
    throw new IllegalArgumentException(s"Unsupported codec ${codec.name}")
  }

  /** True if the inFile still has data to read. Only updated if memoryMap is false. */
  private[common] var inputRemaining = true

  /** The position we are at in the fil. Only used if memoryMap is true. */
  private[common] var totalOffset = 0L

  /** The buffer to read input into and out of. Initialized to empty. */
  private[common] var inBuffer = if (memoryMap) {
    // To avoid the overhead of mapping in the first gigabyte of a file if we're just going to be
    // seeking to a position, use an empty buffer here.
    ByteBuffer.allocate(0)
  } else {
    val buffer = ByteBuffer.allocateDirect(bufferSize)
    buffer.limit(0)
    buffer
  }

  override def hasNext: Boolean = inBuffer.hasRemaining || (inputRemaining && fillBuffer())

  /** Reads and returns a single character from a the file. */
  override def next(): Char = {
    if (inBuffer.hasRemaining || (inputRemaining && fillBuffer())) {
      if (useUtf8) {
        nextUtf8
      } else {
        nextIso8859
      }
    } else {
      throw new NoSuchElementException("next() called on empty ChannelSource")
    }
  }

  /** Positions this source at the given byte offset in the input file. Note that this will allow
    * you to position past the end of the file, in which case no input will be read.
    */
  def position(newPosition: Long): Unit = {
    if (memoryMap) {
      // Truncate the offset to the size of the file.
      totalOffset = if (newPosition > inFile.size) { inFile.size } else { newPosition }
      val size: Long = Math.min(inFile.size - totalOffset, ChannelSource.MaxMapSize)
      inBuffer = inFile.map(FileChannel.MapMode.READ_ONLY, totalOffset, size)
    } else {
      inFile.position(newPosition)
      inBuffer.limit(0)
      inputRemaining = true
    }
  }

  /** @return the current position in the file */
  def position: Long = {
    if (memoryMap) {
      totalOffset + inBuffer.position
    } else {
      inFile.position - inBuffer.remaining
    }
  }

  protected[common] def nextIso8859: Char = {
    // `toChar` doesn't work as expected with bytes; they're first widened with sign extension,
    // meaning that bytes with the high bit set expand into two-byte characters with the high byte
    // set to 0xff. Mask into an int first to fix this.
    (inBuffer.get() & 0xff).toChar
  }

  protected[common] def nextUtf8: Char = {
    // Ensure we are either at the end of file, or have at least three bytes.
    if (inBuffer.remaining() < 3 && inputRemaining) {
      fillBuffer()
    }
    // See https://en.wikipedia.org/wiki/UTF-8 for details. This is checking the first byte for the
    // total length of the encoding.
    val first = inBuffer.get()
    val intVal = if ((first & 0x80) == 0) {
      return first.toChar
    } else if ((first & 0xe0) == 0xc0) {
      // TODO(jkinkead): Secondary bytes (here and below) should all have their two highest bits set
      // as 10. This should be asserted somehow, and if they're corrupted, producing an unknown
      // character instead.
      val second = inBuffer.get()
      ((first & 0x1f) << 6) | (second & 0x3f)
    } else if ((first & 0xf0) == 0xe0) {
      val second = inBuffer.get()
      val third = inBuffer.get()
      ((first & 0x0f) << 12) | ((second & 0x3f) << 6) | (third & 0x3f)
    } else {
      // Anything here is technically illegal UTF-8. However, some sources (i.e. Wikipedia) use
      // wide UTF-8 encodings. This will attempt to skip the correct number of bytes. Note that
      // while we could decode the character here, it will be wider than 16 bits, and therefore
      // won't fit in a JVM Char.

      // skipCount stores the *additional* bytes we will skip, not the total length of the encoding.
      val skipCount = if ((first & 0xf8) == 0xf0) {
        3
      } else if ((first & 0xfc) == 0xf8) {
        4
      } else if ((first & 0xfe) == 0xfc) {
        5
      } else {
        // This will be reached if the first character was truly illegal UTF-8, not just a wide
        // encoding.
        0
      }
      // Technically this is buggy if the buffer is 4 or 5 bytes - we won't skip the full character.
      // That's fine.
      if (inBuffer.remaining() < skipCount && inputRemaining) {
        fillBuffer()
      }
      (0 until skipCount) foreach { _ => if (inBuffer.hasRemaining()) inBuffer.get() }
      // Replace with the unknown character.
      0xfffd
    }
    intVal.toChar
  }

  /** Fills inBuffer from the input file. Sets `inputRemaining` as appropriate.
    * @return true if there are more characters to read in the buffer
    */
  protected[common] def fillBuffer(): Boolean = {
    if (memoryMap) {
      val currOffset = totalOffset + inBuffer.position
      val size: Long = Math.min(inFile.size - currOffset, ChannelSource.MaxMapSize)
      // Only remap if we have any more data to add.
      if (size > inBuffer.remaining) {
        totalOffset = currOffset
        inBuffer = inFile.map(FileChannel.MapMode.READ_ONLY, totalOffset, size)
      }
      inBuffer.hasRemaining
    } else {
      inBuffer.compact()
      val numRead = inFile.read(inBuffer)
      inputRemaining = numRead != -1
      inBuffer.flip()

      inBuffer.hasRemaining
    }
  }

  /** Iterator class for reading the file linewise. */
  private[common] class LineIterator extends Iterator[String] {
    val newline = '\n'.toByte
    val charset = codec.charSet
    override def hasNext: Boolean = ChannelSource.this.hasNext
    var bufferSize = 1024
    var lineBuffer: Array[Byte] = new Array(bufferSize)
    var lineLength = 0

    override def next(): String = {
      if (!ChannelSource.this.hasNext) {
        throw new NoSuchElementException("next() called with no lines remaining")
      }
      lineLength = 0
      var b = inBuffer.get()
      // Read chars until we find a newline.
      while (b != newline && ChannelSource.this.hasNext) {
        if (bufferSize == lineLength) {
          bufferSize = bufferSize * 2
          val newBuffer = new Array[Byte](bufferSize)
          System.arraycopy(lineBuffer, 0, newBuffer, 0, lineBuffer.length)
          lineBuffer = newBuffer
        }
        lineBuffer(lineLength) = b
        lineLength += 1
        b = inBuffer.get()
      }
      // Append final char if we reached the end-of-file.
      if (b != '\n') {
        if (bufferSize == lineLength) {
          bufferSize = bufferSize * 2
          val newBuffer = new Array[Byte](bufferSize)
          System.arraycopy(lineBuffer, 0, newBuffer, 0, lineBuffer.length)
          lineBuffer = newBuffer
        }
        lineBuffer(lineLength) = b
        lineLength += 1
      }
      new String(lineBuffer, 0, lineLength, charset)
    }
  }

  /** Returns an iterator over the remaining lines in this ChannelSource. Note that any reads done
    * on this iterator will be reflected in the main ChannelSource!
    */
  def getLines(): Iterator[String] = new LineIterator
}
object ChannelSource {
  val MaxMapSize: Long = (1L << 31) - 1
  def main(args: Array[String]): Unit = {
    println("Timing Source and ChannelSource.")
    val wikip = "/Users/Kinkead/data/wikipedia/Wikipedia-all.txt-v1"

    def timeMappedChannelSource(): Unit = {
      val startTime = System.currentTimeMillis
      val inFile = FileChannel.open(java.nio.file.Paths.get(wikip))
      val channelSource = new ChannelSource(inFile, memoryMap = true)
      //val charCount = channelSource count { _ => true }
      //println(s"MappedChannelSource charCount = $charCount")
      val lineCount = channelSource count { _ == '\n' }
      println(s"MappedChannelSource lineCount = $lineCount")
      println(s"MappedChannelSource took ${System.currentTimeMillis - startTime} millis.")
      inFile.close()
    }

    def timeChannelSource(): Unit = {
      val startTime = System.currentTimeMillis
      val inFile = FileChannel.open(java.nio.file.Paths.get(wikip))
      val channelSource = new ChannelSource(inFile, bufferSize = 1 << 30)
      //val charCount = channelSource count { _ => true }
      //println(s"ChannelSource charCount = $charCount")
      //val count = channelSource count { _ == '\n' }
      val count = channelSource.getLines().size
      println(s"ChannelSource count = $count")
      println(s"ChannelSource took ${System.currentTimeMillis - startTime} millis.")
      inFile.close()
    }

    def timeSource(): Unit = {
      val startTime = System.currentTimeMillis
      val scalaSource = scala.io.Source.fromFile(wikip)
      //val count = scalaSource count { _ == '\n' }
      val count = scalaSource.getLines().size
      println(s"Scala count = $count")
      println(s"Scala took ${System.currentTimeMillis - startTime} millis.")
      scalaSource.close
    }

    timeChannelSource()
    //timeSource()
    //timeMappedChannelSource()
  }
}
