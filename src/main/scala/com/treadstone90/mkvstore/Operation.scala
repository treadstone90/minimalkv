package com.treadstone90.mkvstore

sealed trait Operation[T]

case class PutOperation[T](key: T, value: Array[Byte]) extends Operation[T]
case class DeleteOperation[T](key: T) extends Operation[T]


case class WriteBatch[T](operations: Operation[T]*) extends Operation[T]

case class InternalPutOperation[T](key: InternalKey[T], value: Array[Byte]) extends Operation[T]
case class InternalDeleteOperation[T](key: InternalKey[T]) extends Operation[T]
