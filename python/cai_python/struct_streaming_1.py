#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pyspark_init as pi
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = pi.sql_init('testAPP', 'local[2]')
