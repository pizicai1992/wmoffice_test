#!/usr/bin/python
# -*- coding: UTF-8 -*-

import re

log_info = '18/10/24 14:31:58 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure'
linfo = re.sub('\d+/\d+/\d+ \d+:\d+:\d+','',log_info)
print linfo


