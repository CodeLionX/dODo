package com.github.codelionx.dodo.types

import scala.collection.{SeqLike, mutable}


/**
  * Enriches [[com.github.codelionx.dodo.types.TypedColumnBase]]s with a seq-like API, similar to
  * [[scala.collection.SeqLike]]
  */
trait TypedColumnSeqLike[T, +Repr] extends SeqLike[Option[T], Repr] {
  self: TypedColumnBase[T] with TypedColumnArrayLike[T] =>

  // already defined in TypedColumnArrayLike
//  override def length: Int = array.length
//  override def apply(idx: Int): T = array.apply(idx)

  override def seq: Seq[Option[T]] = array.seq

  override def iterator: Iterator[Option[T]] = array.iterator

  override protected def newBuilder: mutable.Builder[Option[T], Repr]
}
