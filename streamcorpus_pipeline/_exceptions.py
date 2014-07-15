'''
This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''


class PipelineBaseException(Exception):
    pass

class TransformGivingUp(PipelineBaseException):
    pass

class FailedExtraction(PipelineBaseException):
    pass

class HitMaxi16(PipelineBaseException):
    pass

class PipelineOutOfMemory(PipelineBaseException):
    pass

class TaskQueueUnreachable(PipelineBaseException):
    pass

class GracefulShutdown(PipelineBaseException):
    pass

class ConfigurationError(PipelineBaseException):
    pass

class InvalidStreamItem(PipelineBaseException):
    '''Some content in a stream item was invalid.'''
    pass
