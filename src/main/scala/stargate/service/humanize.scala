package stargate.service

object humanize {
  val KB: Long = 1024
  val MB: Long = 1024 * 1024
  val GB: Long = 1024 * 1024 * 1024

  def bytes(bytes: Long): String = {
    if (bytes < 1024) {
      s"$bytes b"
    } else if (bytes < MB){
      f"${bytes.toFloat / KB.toFloat}%.2f kb"
    } else if (bytes < GB){
      f"${bytes.toFloat / MB.toFloat}%.2f mb"
    } else {
      f"${bytes.toFloat / GB.toFloat}%.2f gb"
    }
  }
}
