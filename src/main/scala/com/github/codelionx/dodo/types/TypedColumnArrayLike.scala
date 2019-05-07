package com.github.codelionx.dodo.types


/**
  * Enriches [[com.github.codelionx.dodo.types.TypedColumnBase]]s with an array-like API.
  */
trait TypedColumnArrayLike[T] {
  self: TypedColumnBase[T] =>

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
  def apply(i: Int): T = array.apply(i)

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
  def update(i: Int, x: T): Unit = array.update(i, x)

  /**
    * @return length of the column
    */
  def length: Int = array.length
}