package com.treadstone90.mkvstore

trait MemTableFactory[T] {
  def emptyMemTable: MemTable[T]
}

class MemTableFactoryImpl[T](dbPath: String, sliceable: Sliceable[T]) extends MemTableFactory[T] {
  def emptyMemTable: MemTable[T] = {
    new MemTableImpl[T](new WriteAheadLogImpl(dbPath), sliceable)
  }
}