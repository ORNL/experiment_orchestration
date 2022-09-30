"""
This file holds the classes used to simplify and organize resource acquisition.
"""

from multiprocessing import (BoundedSemaphore, Semaphore)
import logging

class ResourceWarden:

    def __init__(self, resource_dict, block = False, timeout = None):
        self.resource_dict = resource_dict
        self.block = block
        self.timeout = timeout

    def acquire(self, *resources):
        acquired = list()
        for resource in resources:
            if type(resource) is str:
                success = self.resource_dict[resource].acquire(block = self.block, timeout = self.timeout)
                if success:
                    acquired.append(resource)
            elif type(resource) is dict:
                for r in resource:
                    success = self.resource_dict[r].acquire(**resource[r])
                    if success:
                        acquired.append(r)
            else:
                raise TypeError("Resources must be either str or dict: not {} ({})".format(type(resource), resource))
            if not success:
                self.release(*acquired)
                return list()
        return acquired

    def release(self, *resources):
        released = list()
        for r in resources:
            self.resource_dict[r].release()
            released.append(r)
        return released

class ResourceContainer(ResourceWarden):

    def __init__(self, resource_warden, resources = list()):
        self.resources = resources.copy()
        self.resource_warden = resource_warden

        # this is used to temporarily group consecutively acquired resources
        # together in case they have to be released during exception handling.
        # It is an index in the self.resources list. The marker sits between
        # the resource at the index and the resource before it, meaning the
        # resource at the index is included in the current chunk.
        self.chunk_marker = 0

    def acquire(self, *resources):
        if len(resources) == 0:
            return True
        resources = self.resource_warden.acquire(*resources)
        if resources:
            self.resources += resources
        return resources

    def release(self, index = -1):
        resource = self.resource_warden.release(self.resources[index])
        if resource:
            self.resources.pop(index)

            # If a resource is released which sits behind the chunk marker, set
            # the chunk marker back one.
            if len(self.resources) > 0 and (len(self.resources) + index) % len(self.resources) < self.chunk_marker:
                self.chunk_marker -= 1
                if self.chunk_marker < 0:
                    raise Exception("ResourceContainer chunk_marker somehow dropped below 0.")

        return resource

    def release_chunk(self, num = None):
        released = list()
        if num == None:
            num = len(self.resources) - self.chunk_marker
        for i in range(num):
            released.append(self.release())
        return released

    def reset_chunk_marker(self):
        self.chunk_marker = len(self.resources)

    def release_all(self):
        return self.release_chunk(len(self.resources))
