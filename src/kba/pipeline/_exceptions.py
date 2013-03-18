


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
