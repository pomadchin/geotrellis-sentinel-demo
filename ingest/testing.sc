println("kostas")

import java.io.File

def getListOfFiles(dir: String):List[File] = {
  val d = new File(dir)
  if (d.exists && d.isDirectory) {
    d.listFiles.filter(_.isFile).toList
  } else {
    List[File]()
  }
}

val files = getListOfFiles("/home/kkaralas/Documents/shared/data/t34tel")

println(files.length)


for(geotiff <- files) {
  println("a")
  println(geotiff)
}

files.foreach(file => println(file))
