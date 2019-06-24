package com.github.codelionx.dodo


class DodoException(message: String = "", cause: Throwable = null) extends RuntimeException(message, cause) {
  def this(cause: Throwable) = this(cause.getMessage, cause)
}
