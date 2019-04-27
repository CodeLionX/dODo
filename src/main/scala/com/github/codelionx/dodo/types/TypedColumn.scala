package com.github.codelionx.dodo.types


/**
  * Represents a column of a dataset associated with a specific type. The cell data is stored in the correct
  * (primitive) type.
  *
  * @tparam T the data type
  */
trait TypedColumn[T <: Any] {

  /**
    * Returns the [[com.github.codelionx.dodo.types.DataType]] associated with this column.
    */
  def dataType: DataType[T]

  /**
    * Converts this `TypedColumn` to an [[scala.Array]].
    */
  def toArray: Array[T]

  /**
    * Returns the element on `i`^th^ position.
    *
    * Indices start at `0`; `xs.apply(0)` is the first element of array `xs`.
    * Note the indexing syntax `xs(i)` is a shorthand for `xs.apply(i)`.
    *
    * @param i the index
    * @return the element at the given index
    * @throws ArrayIndexOutOfBoundsException if `i < 0` or `length <= i`
    */
  def apply(i: Int): T

  /**
    * Updates the element at given i.
    *
    * Indices start at `0`; `xs.update(i, x)` replaces the i^th^ element in the array.
    * Note the syntax `xs(i) = x` is a shorthand for `xs.update(i, x)`.
    *
    * @param i the index
    * @param x the value to be written at index `i`
    * @throws ArrayIndexOutOfBoundsException if `i < 0` or `length <= i`
    */
  def update(i: Int, x: T) { throw new Error() }
}
