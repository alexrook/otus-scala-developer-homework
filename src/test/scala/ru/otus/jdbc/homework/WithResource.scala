package ru.otus.jdbc.homework

trait WithResource {

  final def withResource[C <: AutoCloseable, T](resource: C)(process: C => T): T = {
    try {
      process(resource)
    } finally {
      if (resource != null) {
        resource.close()
      }
    }
  }

}
