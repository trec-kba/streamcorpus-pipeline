


class PipelineBaseException(Exception):
    pass

class TransformGivingUp(PipelineBaseException):
    pass

class FailedExtraction(PipelineBaseException):
    pass
