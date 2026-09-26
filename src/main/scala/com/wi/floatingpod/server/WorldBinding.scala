package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{FileAlreadyExistsException, Files, LinkOption, Path, StandardCopyOption, StandardOpenOption}
import java.util.UUID
import scala.util.Try

/** 已认证 binding 与其已验证的 canonical world marker；不授予任何写入能力。 */
final case class ValidatedWorldBinding(binding: PhysicalRecoveryBinding, canonicalMarker: Path):
  require(binding != null && canonicalMarker != null, "validated world binding values must not be null")

/** 读取已解析的 canonical marker，并严格比对其固定二进制 binding 身份。 */
object WorldBinding:
  private val Magic = Array[Byte](0x46, 0x50, 0x57, 0x42)
  private val MarkerSize = Magic.length + Integer.BYTES + java.lang.Long.BYTES * 2

  /**
   * 为调用端提供的 legacy binding 建立固定 canonical marker；既有 marker 仅可由完全相同的 token 与版本重放。
   * 此 seam 不创建 parent、不生成 binding UUID，且不会变更 binding 的 cutover state。
   */
  def createOrRetainMarker(binding: PhysicalRecoveryBinding, canonicalMarker: Path): Try[Path] = Try {
    require(binding != null, "binding must not be null")
    require(canonicalMarker != null, "canonicalMarker must not be null")
    if binding.cutoverState != PhysicalRecoveryCutoverState.LegacyUncertified then
      throw new IllegalStateException("Canonical world marker creation requires a legacy uncertified physical recovery binding")

    if Files.exists(canonicalMarker, LinkOption.NOFOLLOW_LINKS) then
      requireExactMarker(binding, canonicalMarker)
      canonicalMarker
    else
      val parent = canonicalMarker.getParent
      if parent == null then
        throw new IllegalArgumentException(s"Canonical world marker must have an existing parent directory: $canonicalMarker")
      val fileName = canonicalMarker.getFileName
      if fileName == null then
        throw new IllegalArgumentException(s"Canonical world marker must name a file: $canonicalMarker")

      var temporary: Path = null
      try
        temporary = createDurableTemporaryMarker(parent, fileName.toString, markerBytes(binding))
        Files.move(temporary, canonicalMarker, StandardCopyOption.ATOMIC_MOVE)
        temporary = null
        requireExactMarker(binding, canonicalMarker)
        canonicalMarker
      finally
        if temporary != null then Files.deleteIfExists(temporary)
  }

  /**
   * marker 格式固定为：4-byte magic、big-endian int formatVersion、两个 big-endian UUID long。
   * 此 seam 不创建 marker，不生成 binding UUID，也不接受未认证的 cutover state。
   */
  def validate(binding: PhysicalRecoveryBinding, canonicalMarker: Path): Try[ValidatedWorldBinding] = Try {
    require(binding != null, "binding must not be null")
    require(canonicalMarker != null, "canonicalMarker must not be null")
    if binding.cutoverState != PhysicalRecoveryCutoverState.Certified then
      throw new IllegalStateException("World binding requires a certified physical recovery binding")
    requireExactMarker(binding, canonicalMarker)
    ValidatedWorldBinding(binding, canonicalMarker)
  }

  /** 在目标 parent 中以 CREATE_NEW 创建临时文件，并在 publication 前强制同步其内容与元数据。 */
  private def createDurableTemporaryMarker(parent: Path, fileName: String, bytes: Array[Byte]): Path =
    val maximumAttempts = 16
    var attempt = 0
    while attempt < maximumAttempts do
      val candidate = parent.resolve(s".$fileName.${java.lang.Long.toUnsignedString(java.util.concurrent.ThreadLocalRandom.current().nextLong())}.tmp")
      try
        writeDurableMarker(candidate, bytes)
        return candidate
      catch
        case _: FileAlreadyExistsException => attempt += 1
        case failure: Throwable =>
          try Files.deleteIfExists(candidate)
          catch case cleanupFailure: Throwable => failure.addSuppressed(cleanupFailure)
          throw failure
    throw new IOException(s"Unable to reserve a temporary canonical world marker beneath $parent")

  /** 写入完成后强制同步文件内容与元数据，确保后续 atomic publication 不会指向未完成 marker。 */
  private def writeDurableMarker(temporary: Path, bytes: Array[Byte]): Unit =
    val channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    try
      val output = ByteBuffer.wrap(bytes)
      while output.hasRemaining do
        if channel.write(output) <= 0 then throw new IOException(s"Unable to write complete canonical world marker: $temporary")
      channel.force(true)
    finally channel.close()

  /** 以完全固定的 wire 格式序列化调用端提供的 binding，而非生成或转换其身份。 */
  private def markerBytes(binding: PhysicalRecoveryBinding): Array[Byte] =
    ByteBuffer.allocate(MarkerSize)
      .order(ByteOrder.BIG_ENDIAN)
      .put(Magic)
      .putInt(binding.formatVersion)
      .putLong(binding.bindingUuid.getMostSignificantBits)
      .putLong(binding.bindingUuid.getLeastSignificantBits)
      .array()

  /** 仅接受非 symlink 的精确 canonical marker，避免文件类型或 binding 身份被替换。 */
  private def requireExactMarker(binding: PhysicalRecoveryBinding, canonicalMarker: Path): Unit =
    if Files.isSymbolicLink(canonicalMarker) || !Files.isRegularFile(canonicalMarker, LinkOption.NOFOLLOW_LINKS) then
      throw new IllegalStateException(s"Canonical world marker is not a regular file: $canonicalMarker")

    val bytes = Files.readAllBytes(canonicalMarker)
    if bytes.length != MarkerSize then
      throw new IllegalStateException(s"Canonical world marker must contain exactly $MarkerSize bytes")
    val input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    val magic = new Array[Byte](Magic.length)
    input.get(magic)
    if !java.util.Arrays.equals(Magic, magic) then
      throw new IllegalStateException("Canonical world marker magic does not match")
    val formatVersion = input.getInt()
    if formatVersion != binding.formatVersion then
      throw new IllegalStateException(s"Canonical world marker format version $formatVersion does not match binding version ${binding.formatVersion}")
    val markerUuid = new UUID(input.getLong(), input.getLong())
    if markerUuid != binding.bindingUuid then
      throw new IllegalStateException(s"Canonical world marker UUID $markerUuid does not match supplied binding ${binding.bindingUuid}")
