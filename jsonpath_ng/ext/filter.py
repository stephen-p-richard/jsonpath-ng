#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import operator
import re
from six import moves

from .. import JSONPath, DatumInContext, Index


OPERATOR_MAP = {
    '!=': operator.ne,
    '==': operator.eq,
    '=': operator.eq,
    '<=': operator.le,
    '<': operator.lt,
    '>=': operator.ge,
    '>': operator.gt,
    '=~': lambda a, b: True if re.search(b, a) else False,
}


class Filter(JSONPath):
    """The JSONQuery filter"""

    def __init__(self, expression):
        self.expression = expression

    def find(self, datum):
        if not self.expression:
            return datum

        datum = DatumInContext.wrap(datum)

        if isinstance(datum.value, dict):
            datum.value = list(datum.value.values())

        if not isinstance(datum.value, list):
            return []

        result = [DatumInContext(datum.value[i], path=Index(i), context=datum)
                  for i in moves.range(0, len(datum.value)) if self.expression.find(datum.value[i])]

        return result

    def update(self, data, val):
        if type(data) is list:
            for index, item in enumerate(data):
                shouldUpdate = self.expression.find(item)
                if shouldUpdate:
                    if hasattr(val, '__call__'):
                        val.__call__(data[index], data, index)
                    else:
                        data[index] = val
        return data

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.expression)

    def __str__(self):
        return '[?%s]' % self.expression

    def __eq__(self, other):
        return (isinstance(other, Filter)
                and self.expression == other.expression)


class Negation(JSONPath):
    def __init__(self, expression_to_negate):
        self.expression_to_negate = expression_to_negate

    def find(self, datum):
        return not self.expression_to_negate.find(datum)

    def __eq__(self, other):
        return (isinstance(other, Negation) and
                self.expression_to_negate == other.expression_to_negate)

    def __repr__(self):
        return '%s(!%r)' % (self.__class__.__name__, self.expression_to_negate)

    def __str__(self):
        return '!%s' % (self.expression_to_negate)


class Conjunction(JSONPath):
    def __init__(self, left_conjunct, right_conjunct):
        self.left = left_conjunct
        self.right = right_conjunct

    def find(self, datum):
        return False if not self.left.find(datum) else self.right.find(datum)

    def __eq__(self, other):
        return (isinstance(other, Conjunction) and
                ((self.left == other.left and
                  self.right == other.right) or
                 (self.left == other.right and
                  self.right == other.left)))

    def __repr__(self):
        return '%s(%r && %r)' % (self.__class__.__name__, self.left, self.right)

    def __str__(self):
        return '(%s && %s)' % (self.left, self.right)


class Disjunction(JSONPath):
    def __init__(self, left_disjunct, right_disjunct):
        self.left = left_disjunct
        self.right = right_disjunct

    def find(self, datum):
        return True if self.left.find(datum) else self.right.find(datum)

    def __eq__(self, other):
        return (isinstance(other, Disjunction) and
                ((self.left == other.left and
                  self.right == other.right) or
                 (self.left == other.right and
                  self.right == other.left)))

    def __repr__(self):
        return '%s(%r || %r)' % (self.__class__.__name__, self.left, self.right)

    def __str__(self):
        return '(%s || %s)' % (self.left, self.right)


class Expression(JSONPath):
    """The JSONQuery expression"""

    def __init__(self, target, op, value):
        self.target = target
        self.op = op
        self.value = value

    def find(self, datum):
        targetDatum = self.target.find(DatumInContext.wrap(datum))

        if not targetDatum:
            return []
        if self.op is None:
            return targetDatum

        found = []
        comparedValue = self.value
        for data in targetDatum:
            value = data.value
            if isinstance(comparedValue, JSONPath):
                comparedValueDatum = comparedValue.find(DatumInContext.wrap(datum))
                if not comparedValueDatum:
                    return []
                comparedValue = comparedValueDatum[0].value
            elif isinstance(comparedValue, int):
                try:
                    value = int(value)
                except ValueError:
                    continue
            elif isinstance(comparedValue, bool):
                try:
                    value = bool(value)
                except ValueError:
                    continue

            if OPERATOR_MAP[self.op](value, comparedValue):
                found.append(data)

        return found

    def __eq__(self, other):
        return (isinstance(other, Expression) and
                self.target == other.target and
                self.op == other.op and
                self.value == other.value)

    def __repr__(self):
        if self.op is None:
            return '%s(%r)' % (self.__class__.__name__, self.target)
        else:
            return '%s(%r %s %r)' % (self.__class__.__name__,
                                     self.target, self.op, self.value)

    def __str__(self):
        if self.op is None:
            return '%s' % self.target
        else:
            return '%s %s %s' % (self.target, self.op, self.value)
