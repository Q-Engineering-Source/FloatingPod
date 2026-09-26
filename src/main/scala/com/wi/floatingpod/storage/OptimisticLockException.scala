package com.wi.floatingpod.storage

import java.util.Objects
import java.util.UUID

/**
 * 表示基于旧状态版本的写入未影响任何记录，调用方必须重新读取后再决定下一步操作。
 *
 * @param uuid 发生版本冲突的辅助机标识。
 * @param expectedVersion 调用方提交时预期的状态版本。
 */
final class OptimisticLockException(
    val uuid: UUID,
    val expectedVersion: Long
) extends IllegalStateException(
      "Optimistic lock failed for UUID " + Objects.requireNonNull(uuid, "uuid")
        + " at stateVersion " + expectedVersion + "."
    ):
  require(expectedVersion >= 0L, "expectedVersion must not be negative")
