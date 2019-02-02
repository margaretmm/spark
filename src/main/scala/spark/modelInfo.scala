package spark

class ModelInfoScala {
  private var name = ""
  private var version =""
  private var accuracy = ""

  def getName: String = name

  def setName(name: String): Unit = {
    this.name = name
  }

  def getVersion: String = version

  def setVersion(version: String): Unit = {
    this.version = version
  }

  def getAccuracy: String = accuracy

  def setAccuracy(accuracy: String): Unit = {
    this.accuracy = accuracy
  }
}