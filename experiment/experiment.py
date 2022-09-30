"""
This file holds the implementation of the base classes for the framework.
"""

import sys
import logging
import uuid
from .experiment_exceptions import TrialAbort, TrialReset, StageAbort, StageReset, ExperimentAbort, ExperimentReset, Ignore, handle_exceptions
import time

def _create_error_sequence(error_sequence):
    flat_sequence = list()

    # flattens the mixed list of tuples and exception classes to a list of
    # exception classes to make using it easier later on
    for e in error_sequence:

        # If it's not an Exception, it must be a tuple or list.
        if not issubclass(type(e), type):

            # repeat infinitely if tuple[1] == 0
            if e[1] == 0:
                flat_sequence.append(e[0])
                flat_sequence.append(0)

            # add n Exceptions to the sequence
            else:
                flat_sequence += [e[0]] * e[1]

        # Just an exception. Add it to the sequence
        else:
            flat_sequence.append(e)

    return flat_sequence


class Stage:

    """
    error_sequence: List of tuples and exceptions. For functions decorated with
    experiment_exceptions.handle_exceptions, this list is the order that
    exceptions are raised in when an unexpected exception occurs. For example,
    [(StageReset, 3), TrialReset] will cause the stage to reset on the first
    three exceptions. On the fourth, the Trial is signalled to reset. For
    all subsequent exceptions, the exception is raised to the Trial, where its
    error_sequence is used. A tuple with a repeat count of 0 will repeat
    forever. This is useful for preventing errors from elevating to the Trial.
    For example, an error_sequence of [(Ignore, 0)] will ignore unexpected
    exceptions forever.
    """

    def __init__(self, error_sequence = list()):
        self.error_sequence = _create_error_sequence(error_sequence)
        self.original_error_sequence = self.error_sequence.copy()

    def reset_error_sequence(self):
        self.error_sequence = self.original_error_sequence.copy()

    def log(self, *args, **kwargs):
        pass

    def start(self, **kwargs):
        raise NotImplementedError

    def is_done(self, **kwargs):
        raise NotImplementedError

class BlockingStage:

    def __init__(self, error_sequence = list()):
        self.error_sequence = _create_error_sequence(error_sequence)
        self.original_error_sequence = self.error_sequence.copy()

    def reset_error_sequence(self):
        self.error_sequence = self.original_error_sequence.copy()

    def start(self):
        raise NotImplementedError

class TrialResults:

    def __init__(self, data = list(), metadata = dict()):
        self.data = data
        self.metadata = metadata

    def __bool__(self):
        return True

    def add_data(self, data):
        self.data.append(data)

    def edit_metadata(self, key, metadata):
        self.metadata[key] = metadata

    def copy(self):
        return TrialResults(self.data.copy(), self.metadata.copy())

class Trial:

    def __init__(self, stages, initial_state, error_sequence=list(), results = TrialResults()):
        self.error_sequence = _create_error_sequence(error_sequence)
        self.original_error_sequence = self.error_sequence.copy()
        self.state = initial_state.copy()
        self.initial_state = initial_state.copy()
        self.state_checkpoint = initial_state.copy()
        self.stage_index = 0
        self.stages = stages

        # Give each stage a copy of the error sequence
        # This is faster than building an error sequence for every individual
        # stage
        for stage in self.stages:
            stage.error_sequence = stage.error_sequence.copy()

        self.stages = stages
        self.running = False
        self.initial_results = results.copy()
        self.results = self.initial_results.copy()

        # At init, set done to true. This will trigger the experiment to fetch
        # args for the trial and pass them through new_trial, if there are any
        # left. While not 100% intuitive, this greatly simplifies the
        # Experiment init.
        self.done = True
        self.multiple_results = False
        self.errors = 0
        self.fails = 0

    def reset_stage(self):
        self.unset_running()
        self.state = self.state_checkpoint.copy()

    def reset(self):
        self.state = self.initial_state.copy()
        self.state_checkpoint = self.state.copy()
        self.state.update(self.stage_args[0])
        self.done = False
        self.multiple_results = False
        self.stage_index = 0
        self.errors = 0
        self.unset_running()
        self.results = self.initial_results.copy()

        # reset stage error sequences since trials reuse the same stage
        # objects
        #for stage in self.stages:
        #    stage.reset_error_sequence()

    def is_done(self):
        return self.done

    def setup(self, stage_args):
        self.stage_args = stage_args

    def new_trial(self, stage_args = None, **kwargs):
        if stage_args == None:
            stage_args = [{}] * len(self.stages)
        self.error_sequence = self.original_error_sequence.copy()
        self.initial_state.update(kwargs)
        self.state = self.initial_state.copy()
        self.state_checkpoint = self.state.copy()
        self.stage_args = stage_args
        self.state.update(stage_args[0])
        self.done = False
        self.multiple_results = False
        self.stage_index = 0
        self.errors = 0
        self.results = self.initial_results.copy()
        self.unset_running()

        # reset stage error sequences
        for stage in self.stages:
            stage.reset_error_sequence()

    def set_done(self, done = 1):
        self.done = True

    def next_stage(self):
        self.stage_index += 1
        try:
            self.state.update(self.stage_args[self.stage_index])
        except IndexError:
            pass
        self.running = False

    def set_running(self):
        """
        Sets self.running to true.
        """

        self.running = True

    def unset_running(self):
        """
        Sets self.running to false
        """

        self.running = False

    def is_running(self):
        """
        Is the current stage started, or is it waiting to be started?

        Returns:
            true: current stage started
            false: current stage waiting to be started or trial finished
        """

        return self.running

    @handle_exceptions
    def run(self):

        # local variable to track whether the Trial finishes on this call of
        # run.
        just_finished = 0

        # if the Trial is already done, return immediately
        if self.is_done():
            return (1, None)

        current_stage = self.stages[self.stage_index]

        # the current stage of the Trial is not running yet
        if not self.running:

            # create a snapshot of the current state in case the stage has to
            # be restarted
            self.state_checkpoint = self.state.copy()

            # start the current stage

            # for blocking stages, the final results are returned directly by
            # the start method.
            if isinstance(current_stage, BlockingStage):

                # start stage, wait to finish, update state. if success, move on
                try:
                    status, next_state = current_stage.start(**self.state)

                # set status to 1 to continue to the next stage
                except StageAbort:
                    status, next_state = (1, dict())

                # for blocking stages, StageReset == Ignore. set status to 0,
                # and it try again on the next Trial.run
                except (StageReset, Ignore):
                    status, next_state = (0, dict())

                # trial results gets appended rather than overwritten when it
                # appears in next_state
                if "trial_results" in next_state:
                    self.results.add_data(next_state["trial_results"])

                # update the state
                self.state.update(next_state)

                # mark the stage as "just_finished" and setup the next stage
                if status:
                    just_finished = 1
                    self.next_stage()

            # normal stage, just start it
            else:

                # start the stage. if starting succeeds, update status to running
                try:
                    status, next_state = current_stage.start(**self.state)

                # continue to the next stage
                except StageAbort:
                    status, next_state = (0, dict())
                    just_finished = 1
                    self.next_stage()
                    return (0, None)

                # StageReset == Ignore when stage not started
                except (StageReset, Ignore):
                    return (0, None)

                # handle trial_results the same way as with a BlockingStage
                if "trial_results" in next_state:
                    self.results.add_data(next_state["trial_results"])

                # update the state
                self.state.update(next_state)


                # the stage was started successfully.
                if status:
                    self.set_running()

        # stage is running
        else:
            try:
                status, next_state = current_stage.is_done(**self.state)

            # continue to the next stage
            except StageAbort:
                status, next_state = (0, dict())
                just_finished = 1
                self.next_stage()
                return (0, None)

            # reset the state to its value at the beginning of this stage
            # and restart it.
            except StageReset:
                self.reset_stage()
                return (0, None)

            # do nothing, return not done
            except Ignore:
                return (0, None)

            if "trial_results" in next_state:
                self.results.add_data(next_state["trial_results"])

            # update the state
            self.state.update(next_state)

            # stage is done
            if status:
                just_finished = 1
                self.next_stage()
                self.unset_running()

        # If this is the last stage and the stage just finished mark as done
        # and return the results.
        if self.stage_index == len(self.stages) and just_finished:
            self.done = True
            return (1, self.results)
        else:
            return (0, None)

    def start(self, *args, **kwargs):
        self.current_stage = stages[0]
        self.current_stage.start(*args, **kwargs)

class Experiment:

    """
    The experiment class holds a list of Trial objects. The experiment runs
    in a single thread and iterates through each trial, checking if it's
    finished. If it is, it collects and ships the results and starts a new
    trial in its place, using the arguments for the next trial.
    """

    def __init__(self, base_trial_config = dict(), per_instance_config = list(), trial_arg_configs = list(), stages = [], trial_class=Trial, sleep_time=0.01, error_sequence = list(), trial_error_sequence = list(), stage_error_sequence = list()):

        # build stage objects with error_sequences
        def build_stage_objects(stages, stage_error_sequence):

            # make sure the error sequence is all Exception classes
            # or tuples/lists: (Exception, int)
            def check_error_sequence(error_sequence):
                for _e in error_sequence:
                    e = _e # break reference
                    if isinstance(e, (list, tuple)):
                        if len(e) != 2 or not isinstance(e[1], int):
                            raise TypeError("Invalid error sequence: {} is "
                                            "not a valid error tuple."\
                                            .format(e))
                        e = e[0]
                    if not issubclass(e, Exception):
                        raise TypeError("Invalid error sequence: {} is not a "
                                        "subclass of Exception or list/tuple"\
                                        .format(e))

            def check_list_of_error_sequence(error_sequence):
                for l in error_sequence:
                    if not isinstance(l, (list, tuple)):
                        raise TypeError("Invalid error sequence: {} is not a "
                                        "list/tuple of lists/tuples or a list of "
                                        "Exception "
                                        "classes/subclasses.".format(error_sequence))
                    check_error_sequence(l)

            stage_objects = list()

            # if stage_error_sequence is a defined per stage
            if stage_error_sequence and \
            isinstance(stage_error_sequence[0], (list, tuple)) and \
            len(stage_error_sequence) == len(stages):
                check_list_of_error_sequence(stage_error_sequence)
                for stage_class, error_sequence in zip(stages, stage_error_sequence):
                    error_sequence = _create_error_sequence(error_sequence)
                    stage_objects.append(stage_class(error_sequence.copy()))

            # if stage_error_sequence is defined once
            elif stage_error_sequence:

                check_error_sequence(stage_error_sequence)
                stage_error_sequence = _create_error_sequence(stage_error_sequence)
                for stage_class in stages:
                    stage_objects.append(stage_class(stage_error_sequence.copy()))

            else:
                for stage_class in stages:
                    stage_objects.append(stage_class())

            return stage_objects

        # end build_stage_objects

        self.trial_instances = list()
        self.base_trial_config = base_trial_config
        self.per_instance_config = per_instance_config
        self.trial_arg_configs = trial_arg_configs
        self.stages = stages.copy()
        self.sleep_time = sleep_time
        self.error_sequence = _create_error_sequence(error_sequence)

        # merge instance configs and base configs for each trial instance, then
        # create and add the new trial instance to the experiment

        # if a trial class is given per config
        if type(trial_class) is list:
            if len(per_instance_config) == len(trial_class):
                for instance, c in zip(per_instance_config, trial_class):
                    if not issubclass(trial_class, Trial):
                        # TODO descriptive error
                        raise
                    trial_config = base_trial_config.copy()
                    trial_config.update(instance)
                    stage_objects = build_stage_objects(stages, stage_error_sequence.copy())
                    self.trial_instances.append(c(stage_objects, trial_config, error_sequence = trial_error_sequence.copy()))

            else:
                # TODO number of classes must match number of instances
                raise

        else:

            if not issubclass(trial_class, Trial):
                raise TypeError("Class {} is not a subclass of Trial".format(trial_class))

            for instance in per_instance_config:
                trial_config = base_trial_config.copy()
                trial_config.update(instance)
                stage_objects = build_stage_objects(stages, stage_error_sequence.copy())
                self.trial_instances.append(trial_class(stage_objects, trial_config, error_sequence = trial_error_sequence.copy()))

        self.errors = [0] * len(self.trial_instances)

    def ship_results(self, results, **kwargs):
        #print(results.data, results.metadata)
        pass

    def log_error(self, state):
        #print(state)
        pass

    @handle_exceptions
    def run(self):
        trial_index = 0
        done = False

        # while there are still trials to start
        while len(self.trial_arg_configs) > 0:
            trial = self.trial_instances[trial_index]

            # run the trial, handle all relevant exceptions
            try:
                status, results = trial.run()

            # reset the trial and try again
            except TrialReset:
                status = 0
                results = trial.results

                # grab partial results if they're there
                # if "trial_results" in trial.state:
                #     results = trial.state["trial_results"]
                # else:
                #     results = 0

                trial.reset()

            # continue to the next trial
            except TrialAbort:
                status = 1
                results = trial.results

                # grab partial results if they're there
                # if "trial_results" in trial.state:
                #     results = trial.state["trial_results"]
                # else:
                #     results = 0

            # ignore the exception, hope it resolves itself in the future
            except Ignore:
                status, results = (0, 0)

            # results were provided by the run
            if results:
                self.ship_results(results)

            # if status > 0, the trial is finished. continue to the next set of
            # args.
            if status:
                copy = self.base_trial_config.copy()
                instance_config = self.per_instance_config[trial_index]

                # if there're any trials left to complete, start the next one
                if len(self.trial_arg_configs) > 0:
                    trial.new_trial(**copy, **instance_config, **self.trial_arg_configs.pop())

            # increment trial index
            trial_index = (trial_index + 1) % len(self.trial_instances)

            # sleep to help rate limit
            time.sleep(self.sleep_time)

        # while there are still trials running after all have been started
        while not done:

            #check each trial to see if it's done
            done = True
            for trial, trial_index in zip(self.trial_instances, range(len(self.trial_instances))):


                # run the trial, handle all relevant exceptions
                try:
                    status, results = trial.run()

                # reset the trial and try again
                except TrialReset:
                    status = 0
                    results = trial.results # send partial results
                    trial.reset()

                # mark the trial as done and continue
                except TrialAbort:
                    status = 1
                    results = trial.results # send partial results
                    trial.set_done()

                # ignore the exception, hope it resolves itself in the future
                except Ignore:
                    status, results = (0, 0)

                # results were returned
                if results:
                    self.ship_results(results)

                # if status is false, the trial is still incomplete.
                if not status:
                    done = False

            # sleep to rate limit
            time.sleep(self.sleep_time)
