package org.allenai.common

import org.allenai.common.testkit.UnitSpec

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Files, StandardOpenOption }
import java.util.NoSuchElementException

class ChannelSourceSpec extends UnitSpec {
  /** @return a channel open to a new temp file containing the given bytes */
  def newFileWithBytes(bytes: Array[Byte]): FileChannel = {
    val path = Files.createTempFile(null, null)
    val channel = FileChannel.open(
      path,
      StandardOpenOption.DELETE_ON_CLOSE,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE
    )
    val buffer = ByteBuffer.allocate(bytes.length)
    buffer.put(bytes)
    buffer.rewind
    channel.write(buffer)
    channel.position(0)
    channel
  }

  "ChannelSource" should "read a simple file" in {
    val foo = newFileWithBytes(Array('f', 'o', 'o') map { _.toByte })

    val source = new ChannelSource(foo)
    source.hasNext should be(true)
    source.next() should be('f')
    source.hasNext should be(true)
    source.next() should be('o')
    source.hasNext should be(true)
    source.next() should be('o')
    source.hasNext should be(false)
    intercept[NoSuchElementException] {
      source.next()
    }
  }

  it should "read a file in UTF-8 with a small buffer that crosses character boundaries" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })

    // This buffer size will cross character boundaries (the first read will hit the middle of the
    // second ü).
    val source = new ChannelSource(foomlaut, bufferSize = 4)
    source.mkString("") should be("füü")
  }

  it should "report the correct byte position" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })
    val source = new ChannelSource(foomlaut, bufferSize = 4)

    source.position should be(0)
    source.next()
    source.position should be(1)
    source.next()
    source.position should be(3)
    source.next()
    source.position should be(5)
  }

  it should "allow positioning in a file" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })
    val source = new ChannelSource(foomlaut, bufferSize = 4)

    source.position(3)
    source.next() should be('ü')
    source.position should be(5)
    source.hasNext should be(false)

    source.position(0)
    source.mkString("") should be("füü")
  }
}
