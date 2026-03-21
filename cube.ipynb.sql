-- Databricks notebook source
-- MAGIC %python
-- MAGIC class cube:
-- MAGIC     def __init__(self, edge):
-- MAGIC         self.edge = edge
-- MAGIC
-- MAGIC     def calculate_volume(self):
-- MAGIC         return self.edge*self.edge
-- MAGIC     
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC c = cube(3)
-- MAGIC c.calculate_volume()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sys
-- MAGIC sys.path.append("/Workspace/Users/hma68@hotmail.com")
-- MAGIC
-- MAGIC from py_library import cube
-- MAGIC
-- MAGIC c = cube.cube(3)
-- MAGIC c.calculate_volume()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sys
-- MAGIC sys.path.append("/Workspace/Users/hma68@hotmail.com")
-- MAGIC from py_library.cube import cube_py
-- MAGIC c = cube_py(3)
-- MAGIC
-- MAGIC c.calculate_volume()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC print(os.path.abspath('.'))
-- MAGIC print(sys.path)
-- MAGIC

-- COMMAND ----------

