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

  /** Stores füü, in UTF-8. */
  val foomlaut: Array[Byte] = Array('f'.toByte, 0xc3, 0xbc, 0xc3, 0xbc) map { _.toByte }

  /** @return a channel open to a new temp file containing the given chars */
  def newFileWithChars(chars: Iterable[Char]): FileChannel = {
    newFileWithBytes(chars.toArray map { _.toByte })
  }

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
    val foo = newFileWithChars("foo")

    val source = new ChannelSource(foo)
    source.mkString should be("foo")
  }

  it should "read a simple file in ISO-8859-1" in {
    val foo = newFileWithChars("foo")

    val source = new ChannelSource(foo)(Codec.ISO8859)
    source.mkString should be("foo")
  }

  it should "read a file in UTF-8 with a small buffer that crosses character boundaries" in {
    val testfile = newFileWithBytes(foomlaut)

    // This buffer size will cross character boundaries (the first read will hit the middle of the
    // second ü).
    val source = new ChannelSource(testfile, bufferSize = 4)
    source.mkString should be("füü")
  }

  it should "report the correct byte position when reading wide chars" in {
    val testfile = newFileWithBytes(foomlaut)
    val source = new ChannelSource(testfile, bufferSize = 4)

    withClue("Start position: ") { source.position should be(0) }
    source.next()
    withClue("After reading single-byte char: ") { source.position should be(1) }
    source.next()
    withClue("After reading two-byte char: ") { source.position should be(3) }
    source.next()
    withClue("After reading another two-byte char: ") { source.position should be(5) }
  }

  it should "allow positioning in a file" in {
    val testfile = newFileWithBytes(foomlaut)
    val source = new ChannelSource(testfile, bufferSize = 4)

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
    // Mu, encoded in ISO-8859-1.
    val catsSay = newFileWithBytes(Array(0xb5.toByte))
    val source = new ChannelSource(catsSay)(Codec.ISO8859)

    source.mkString("") should be("µ")
  }

  "ChannelSource.getLines" should "read lines from a file" in {
    val newlines = newFileWithChars("a\nb")
    val source = new ChannelSource(newlines)
    val lines = source.getLines()
    withClue("The first line: ") { lines.next() should be("a") }
    withClue("Position after reading the first line: ") { source.position should be(2) }
    withClue("The second line: ") { lines.next() should be("b") }
    withClue("Position after reading the second line: ") { source.position should be(3) }
    lines.hasNext should be(false)
  }

  it should "handle lines that span buffer boundaries" in {
    val newlines = newFileWithChars("abcde\nfghij\n")
    val source = new ChannelSource(newlines, 4)
    val lines = source.getLines()

    lines.next() should be("abcde")
    withClue("Position after reading the first line: ") { source.position should be(6) }
    lines.next() should be("fghij")
    withClue("Position after reading the second line: ") { source.position should be(12) }
    lines.hasNext should be(false)
  }

  it should "handle empty lines" in {
    val newlines = newFileWithChars("\n\na\n\n")
    val source = new ChannelSource(newlines)
    val lines = source.getLines()

    lines.next() should be("")
    lines.next() should be("")
    lines.next() should be("a")
    lines.next() should be("")
    lines.hasNext should be(false)
  }

  it should "handle mixing getLines with getting chars" in {
    val newlines = newFileWithChars("foo\nbar")
    val source = new ChannelSource(newlines)
    val lines = source.getLines()

    source.next() should be('f')
    lines.next() should be("oo")
    source.next() should be('b')
    lines.next() should be("ar")
  }

  it should "handle carriage returns correctly" in {
    val windowsNewlines = newFileWithChars("foo\rbar\r\ngaz\r")
    val source = new ChannelSource(windowsNewlines)
    val lines = source.getLines()

    lines.next() should be("foo")
    lines.next() should be("bar")
    lines.next() should be("gaz")
    lines.hasNext should be(false)
  }
}
