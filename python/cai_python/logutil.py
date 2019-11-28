#!/usr/bin/python
# -*- coding: UTF-8 -*-
import logging,os
import uuid

class LogUtil():
    """日志记录工具类.

    通过标准的Logging模块，将日志按照相应的级别记录到文件中,同时输出在屏幕上

    Attributes:
        log_file: A file used to record content
        logger: a instance of logging
        level: log level
    Args:
        logfile: File name with path
        level: log level (debug,info,warn,error,critical);default info
    """
    level_relations = {
        'debug': logging.DEBUG, 'info': logging.INFO, 'warn': logging.WARNING,
        'error': logging.ERROR, 'critical': logging.CRITICAL
    }
    def __init__(self,logfile,level='info'):
        self.log_file = logfile
        if os.access(self.log_file, os.F_OK):
            os.remove(self.log_file)
        _logname = str(uuid.uuid4()).replace('-','')
        self.logger = logging.getLogger(_logname)
        self.logger.setLevel(self.level_relations.get(level.lower()))
        self.fh = logging.FileHandler(self.log_file)
        # self.fh.setLevel(logging.INFO)
        self.ch = logging.StreamHandler()
        # self.ch.setLevel(logging.INFO)
        self.formatter = logging.Formatter('[%(asctime)s - %(levelname)s]: %(message)s')
        self.fh.setFormatter(self.formatter)
        self.ch.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.logger.addHandler(self.ch)

    def info(self, info_mess):
        self.logger.info(info_mess)

    def error(self, error_mess):
        self.logger.error(error_mess)
    
    def warn(self, warn_mess):
        self.logger.warn(warn_mess)
    
    def debug(self, debug_mess):
        self.logger.debug(debug_mess)