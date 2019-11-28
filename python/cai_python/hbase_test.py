#!/usr/bin/python
# -*- coding: UTF-8 -*-

import happybase

connection = happybase.Connection('cdhdev1')
print connection.tables()
