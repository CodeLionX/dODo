package com.github.codelionx.dodo.types

object Implicits {

  implicit class RichTypedColumnArray[T](ta: Array[TypedColumn[T]]) {

    def prettyPrint: String =
      s"""|Relation
          |===============================
          |${ta.map(col => col.toString).mkString("\n")}
          |""".stripMargin
  }

}
