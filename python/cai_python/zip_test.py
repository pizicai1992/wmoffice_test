# -*- coding: UTF-8 -*-

import zipfile
import os 

PATH='/var/lib/impala/test/cai_file/file/sms_userid_20200617'
zfName = os.path.join(PATH, 'sms_userid.zip')

zf = zipfile.ZipFile(zfName, 'w', zipfile.ZIP_DEFLATED)
for file in os.listdir(PATH):
    if file.endswith('csv'):
        print file 
        zfile = os.path.join(PATH, file)
        zf.write(zfile)

zf.close()
