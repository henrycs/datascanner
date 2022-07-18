#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Interface for quotes worker
"""
import datetime
from abc import ABC
from typing import List, Optional, Union

import numpy
from coretypes import Frame, FrameType


class QuotesFetcher(ABC):
    async def create_instance(self, **kwargs):
        raise NotImplementedError
