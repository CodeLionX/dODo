package com.github.codelionx.dodo.types


trait TypedColumn[T <: Any] {

  def toArray: Array[T]

  def apply(index: Int): T

}
