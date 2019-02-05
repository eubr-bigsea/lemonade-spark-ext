package br.ufmg.dcc.lemonade.ext.csv

/**
  * Created by walter on 14/05/18.
  */

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

import scala.util.control.NonFatal

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    try {
      out.defaultWriteObject()
      value.write(out)
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    try {
      value = new Configuration(false)
      value.readFields(in)
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw new IOException(e)
    }
  }
}