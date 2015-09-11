package org.allenai.common

import org.allenai.common.testkit.UnitSpec

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Files, StandardOpenOption }
import java.util.NoSuchElementException

import scala.io.Codec

class ChannelSourceSpec extends UnitSpec {
  // UTF-8 is the default encoding for these tests.
  implicit val codec = Codec.UTF8

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

  "ChannelSource" should "read a simple file in UT8" in {
    val foo = newFileWithBytes(Array('f', 'o', 'o') map { _.toByte })

    val source = new ChannelSource(foo)
    source.mkString should be("foo")
  }

  it should "read a simple file in ISO-8859-1" in {
    val foo = newFileWithBytes(Array('f', 'o', 'o') map { _.toByte })

    val source = new ChannelSource(foo)(Codec.ISO8859)
    source.mkString should be("foo")
  }

  it should "read a file in UTF-8 with a small buffer that crosses character boundaries" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })

    // This buffer size will cross character boundaries (the first read will hit the middle of the
    // second ü).
    val source = new ChannelSource(foomlaut, bufferSize = 4)
    source.mkString should be("füü")
  }

  it should "report the correct byte position when reading wide chars" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })
    val source = new ChannelSource(foomlaut, bufferSize = 4)

    withClue("Start position: ") { source.position should be(0) }
    source.next()
    withClue("After reading single-byte char: ") { source.position should be(1) }
    source.next()
    withClue("After reading two-byte char: ") { source.position should be(3) }
    source.next()
    withClue("After reading another two-byte char: ") { source.position should be(5) }
  }

  it should "allow positioning in a file" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })
    val source = new ChannelSource(foomlaut, bufferSize = 4)

    source.position(3)
    withClue("After scanning to final character: ") { source.next() should be('ü') }
    withClue("After reading the final character: ") {
      source.position should be(5)
      source.hasNext should be(false)
    }

    source.position(0)
    withClue("After reading and resetting: ") { source.mkString("") should be("füü") }
  }

  it should "read high-valued ISO-8859-1 characters" in {
    val catsSay = newFileWithBytes(Array(0xb5.toByte))
    val source = new ChannelSource(catsSay)(Codec.ISO8859)

    source.mkString("") should be("µ")
  }

  it should "read a simple memory-mapped file" in {
    val foo = newFileWithBytes(Array('f', 'o', 'o') map { _.toByte })

    val source = new ChannelSource(foo, memoryMap = true)
    source.mkString should be("foo")
  }

  it should "report the correct position in a memory-mapped file" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })
    val source = new ChannelSource(foomlaut, memoryMap = true)

    withClue("Start position: ") { source.position should be(0) }
    source.next()
    withClue("After reading single-byte char: ") { source.position should be(1) }
    source.next()
    withClue("After reading two-byte char: ") { source.position should be(3) }
    source.next()
    withClue("After reading another two-byte char: ") { source.position should be(5) }
  }

  it should "allow positioning in a memory-mapped file" in {
    // Array stores füü, in UTF-8.
    val foomlaut = newFileWithBytes(Array('f', 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte })
    val source = new ChannelSource(foomlaut, memoryMap = true)

    source.position(3)
    withClue("After scanning to final character: ") { source.next() should be('ü') }
    withClue("After reading the final character: ") {
      source.position should be(5)
      source.hasNext should be(false)
    }

    source.position(0)
    withClue("After reading and resetting: ") { source.mkString("") should be("füü") }
  }

  it should "read lines from a file" in {
    val newlines = newFileWithBytes(Array('a', '\n', 'b') map { _.toByte })
    val source = new ChannelSource(newlines)
    val lines = source.getLines()
    withClue("The first line: ") { lines.next() should be("a") }
    withClue("After reading the first line: ") { source.position should be(2) }
    withClue("The second line: ") { lines.next() should be("b") }
  }
}
