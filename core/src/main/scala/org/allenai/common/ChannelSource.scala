package org.allenai.common

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.NoSuchElementException

/** A class that combines the best elements of java.nio.channels.FileChannel and scala.io.Source.
  * This provides:
  * <ul>
  * <li>byte-valued position in the file
  * <li>seekability (seek-to-position)
  * <li>character iteration
  * <li>optional memory-mapping of files for fast reading
  * </ul>
  * Memory-mapped files will be read in chunks of one gigabyte.
  * @param inFile the file channel to wrap
  * @param bufferSize the size of the internal buffer to use
  */
class ChannelSource(
    inFile: FileChannel,
    bufferSize: Int = 1024,
    memoryMap: Boolean = true
) extends Iterator[Char] {
  require(bufferSize >= 3, "Buffer must be at least 3 bytes to decode UTF-8!")

  // TODO: Handle memoryMap parameter!!!
  var inputRemaining = true

  val inBuffer = ByteBuffer.allocate(bufferSize)
  inBuffer.limit(0)

  override def hasNext: Boolean = inBuffer.hasRemaining || (inputRemaining && fillBuffer())

  /** Reads and returns a single UTF-8 character from a the file. */
  override def next(): Char = {
    if (inBuffer.hasRemaining || (inputRemaining && fillBuffer())) {
      nextUtf8
    } else {
      throw new NoSuchElementException("next() called on empty ChannelSource")
    }
  }

  /** Positions this source at the given byte offset in the input file. Note that this will allow
    * you to position past the end of the file, in which case no input will be read.
    */
  def position(newPosition: Long): Unit = {
    inFile.position(newPosition)
    inBuffer.limit(0)
    inputRemaining = true
  }

  /** @return the current position in the file */
  def position: Long = inFile.position - inBuffer.remaining

  protected[common] def nextUtf8: Char = {
    // Ensure we are either at the end of file, or have at least three bytes.
    if (inBuffer.remaining() < 3 && inputRemaining) {
      fillBuffer()
    }
    val first = inBuffer.get()
    val intVal = if ((first & 0x80) == 0) {
      return first.toChar
    } else if (((first & 0xe0) ^ 0xc0) == 0) {
      val second = inBuffer.get()
      ((first & 0x1f) << 6) | (second & 0x3f)
    } else if (((first & 0xf0) ^ 0xe0) == 0) {
      val second = inBuffer.get()
      val third = inBuffer.get()
      ((first & 0x0f) << 12) | ((second & 0x3f) << 6) | (third & 0x3f)
    } else {
      throw new Exception("JVM can only handle 16-bit unicode")
    }
    intVal.toChar
  }

  /** Fills inBuffer from the input file. Sets `inputRemaining` as appropriate.
    * @return true if any characters are left in the file channel
    */
  protected[common] def fillBuffer(): Boolean = {
    inBuffer.compact()
    val numRead = inFile.read(inBuffer)
    inputRemaining = numRead != -1
    inBuffer.flip()

    inBuffer.hasRemaining
  }
}
object ChannelSource {
}
