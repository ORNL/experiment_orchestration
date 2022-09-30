"""
This file has the decorator @handle_exceptions which are used for the generic
exception handling of the framework.
"""

import logging
from functools import wraps

"""
Exception classes and decorators for clean exception handling in teh AI-ATAC
framework.
"""

class ExperimentException(Exception):
    """
    Base exception class for the AI-ATAC Experiment framework. Level sets the
    logging level the exception will be logged on.

    For reference:

    50: CRITICAL
    40: ERROR
    30: WARNING
    20: INFO
    10: DEBUG
    """

    def __init__(self, level=40, **kwargs):
        self.level = level
        self.__dict__.update(kwargs)

class ExperimentAbort(ExperimentException):
    """
    Something unrecoverable has happened. Exit the experiment.
    """
    pass

class ExperimentReset(ExperimentException):
    """
    Restart the Experiment from the first trial.
    """
    pass

class TrialAbort(ExperimentException):
    """
    Give up and continue to the next trial. Only gets caught when raised from
    Trial or Stage objects.
    """
    pass

class TrialReset(ExperimentException):
    """
    Reset and try the Trial again. Only gets caught when raised from Trial or
    Stage objects.
    """
    pass

class StageAbort(ExperimentException):
    """
    Continue to the next stage of the trial. Only gets caught when raised from
    a Stage.start or stage.is_done.
    """
    pass

class StageReset(ExperimentException):
    """
    Reset the current stage and try again. Only gets caught when raised from a
    Stage.start or Stage.is_done
    """
    pass

class Ignore(ExperimentException):
    """
    Log the exception and take no action.
    """
    pass

# decorator to add automatic exception handling to the functions of either an
# Experiment, Trial, or Stage subclass.
def handle_exceptions(func):
    @wraps(func)
    def try_function(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
            
        # if any of these exceptions are raised manually, bypass the error
        # sequence
        except (TrialAbort, TrialReset, StageAbort, StageReset):
            raise

        except Exception as err:
            logging.exception(err)

            # if error sequence is empty, elevate the exception
            if not self.error_sequence:
                raise

            # A zero following an Exception type signals that the Exception
            # should be raised indefinitely
            elif len(self.error_sequence) >1 and self.error_sequence[1] ==0:
                raise self.error_sequence[0]

            # By default, pop the exception class off error_sequence and
            # raise it
            else:
                raise self.error_sequence.pop(0)
    return try_function
