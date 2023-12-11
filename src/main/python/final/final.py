#!/usr/bin/env python3
"""
Description: Reimplementation and extension of reframe by Brad Miller (https://github.com/bnmnetp/reframe).
Authors: Samuel Vue, Wolfgang Baldus, and Adam Mertzenich
"""

from __future__ import annotations

from typing import Optional

import csv

import pytest

from collections import namedtuple

from operator import attrgetter


class GroupWrap:
    def __init__(self, allGrouped, cols):
        self.allGrouped = allGrouped
        self.cols = cols

    def count(self, col):
        """
        Count the number of occurrences of a value in the column for a group.

        :param col:
        :return:  A Relation with the groupby column(s) and count for a single column

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> groupBy = (rel.groupby(["GradYear"]).count("SName").project(["GradYear", "count_SName"]).print())
        GradYear    count_SName
        2021       3
        2020       3
        2022       2
        2019       1

        >>>
        """
        new_cols = tuple([self.cols[0], f"count_{col}"])
        new_row = namedtuple("Row", new_cols)
        counts = {}
        for row in self.allGrouped.rows:
            key = getattr(row, self.cols[0])
            counts[key] = counts.get(key, 0) + 1

        results = [new_row(key, str(counts[key])) for key in counts.keys()]
        return Relation(rows=frozenset(results), cols=new_cols)

    def sum(self, col):
        """
        Count the sum of occurrences of a value in the column for a group.

        :param col:
        :return:  A Relation with the groupby column(s) and count for a single column

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> groupBy = (rel.groupby(["GradYear"]).sum("MajorId").project(["GradYear", "sum_MajorId"]).print())
        GradYear    sum_MajorId
        2021       50
        2022       30
        2020       70
        2019       20

        >>>
        """
        new_cols = tuple([self.cols[0], f"sum_{col}"])
        new_row = namedtuple("Row", new_cols)
        sums = {}
        for row in self.allGrouped.rows:
            key = getattr(row, self.cols[0])
            sums[key] = sums.get(key, 0) + int(getattr(row, col))

        result = [new_row(key, str(sums[key])) for key in sums]
        return Relation(rows=frozenset(result), cols=new_cols)

    def max(self, col):
        """
        Find the maximum value in the column for the Relation

        :param col:
        :return:  A Relation with the groupby column(s) and count for a single column

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> groupBy = (rel.groupby(["GradYear"]).max("MajorId").project(["GradYear", "max_MajorId"]).print())
        GradYear    max_MajorId
        2021       30
        2022       20
        2019       20
        2020       30

        >>>
        """
        new_cols = tuple([self.cols[0], f"max_{col}"])
        new_row = namedtuple("Row", new_cols)
        max_values = {}
        for row in self.allGrouped.rows:
            key = getattr(row, self.cols[0])
            current_value = float(getattr(row, col))
            max_values[key] = int(
                max(max_values.get(key, float("-inf")), current_value)
            )

        result = [new_row(key, str(max_values[key])) for key in max_values]
        return Relation(rows=frozenset(result), cols=new_cols)

    def min(self, col):
        """
        Find the minimum value in the column for the Relation

        :param col:
        :return:  A Relation with the groupby column(s) and count for a single column

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> groupBy = (rel.groupby(["GradYear"]).min("MajorId").project(["GradYear", "min_MajorId"]).print())
        GradYear    min_MajorId
        2022       10
        2020       20
        2019       20
        2021       10

        >>>
        """
        new_cols = tuple([self.cols[0], f"min_{col}"])
        new_row = namedtuple("Row", new_cols)
        min_values = {}
        for row in self.allGrouped.rows:
            key = getattr(row, self.cols[0])
            current_value = float(getattr(row, col))
            min_values[key] = int(
                min(min_values.get(key, float("1e15")), current_value)
            )

        result = [new_row(key, str(min_values[key])) for key in min_values]
        return Relation(rows=frozenset(result), cols=new_cols)

    def mean(self, col):
        """
        Find the average/mean of the column in the Relation

        :param col:
        :return:  A Relation with the groupby column(s) and count for a single column

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> groupBy = (rel.groupby(["MajorId"]).mean("GradYear").project(["MajorId", "mean_GradYear"]).print())
        MajorId    mean_GradYear
        10       2021.33
        20       2020.25
        30       2020.5

        >>>
        """
        new_cols = tuple([self.cols[0], f"mean_{col}"])
        new_row = namedtuple("Row", new_cols)
        sum_values = {}
        count_values = {}
        for row in self.allGrouped.rows:
            key = getattr(row, self.cols[0])
            current_value = int(getattr(row, col))
            sum_values[key] = sum_values.get(key, 0) + current_value
            count_values[key] = count_values.get(key, 0) + 1

        # Calculate mean for each group
        result = [
            new_row(
                key,
                str(round(sum_values[key] / count_values[key], 2))
                if key in count_values and count_values[key] > 0
                else None,
            )
            for key in sum_values
        ]

        return Relation(rows=frozenset(result), cols=new_cols)

    def median(self, col):
        """
        Find the median of values in the column for the group

        :param col:
        :return:  A Relation with the groupby column(s) and count for a single column

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> groupBy = (rel.groupby(["GradYear"]).median("MajorId").project(["GradYear", "median_MajorId"]).print())
        GradYear    median_MajorId
        2020       20
        2021       10
        2022       15.0
        2019       20

        >>>
        """
        new_cols = tuple([self.cols[0], f"median_{col}"])
        new_row = namedtuple("Row", new_cols)
        values_per_group = {}
        for row in self.allGrouped.rows:
            key = getattr(row, self.cols[0])
            current_value = getattr(row, col)
            if key not in values_per_group:
                values_per_group[key] = []
            values_per_group[key].append(int(current_value))

        # Calculate median for each group
        result = [
            new_row(
                key,
                str(self.calculate_median(values)) if key in values_per_group else None,
            )
            for key, values in values_per_group.items()
        ]

        return Relation(rows=frozenset(result), cols=new_cols)

    def calculate_median(self, values):
        sorted_values = sorted(values)
        n = len(sorted_values)
        if n % 2 == 0:
            middle = n // 2
            return (sorted_values[middle - 1] + sorted_values[middle]) / 2
        else:
            return sorted_values[n // 2]

    def print(self):
        print("    ".join([field for field in self.cols]))
        for row in self.allGrouped.rows:
            values = "    ".join(str(getattr(row, col)) for col in self.cols)
            print(values)


class Relation:
    rows: frozenset = frozenset()
    cols: tuple = tuple()

    def __init__(
        self,
        rows: Optional[frozenset] = None,
        cols: Optional[tuple] = None,
        filepath=None,
        sep=",",
    ):
        if filepath:
            self.create_relation(filepath, sep)
        elif (rows != None) and (cols != None):
            self.rows = rows
            self.cols = cols

    def create_relation(self, path, sep):
        with open(path, newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=sep, quotechar="|")
            self.cols = tuple(next(reader))
            row_entry = namedtuple("Row", self.cols)
            self.rows = frozenset(row_entry(*row) for row in reader)

    def print(self):
        """
        Prints out the table in a human readable format.
        Taken from https://stackoverflow.com/a/13214945
        """
        cleaned_rows = [list(self.cols)]
        for row in self.rows:
            new_row = []
            for i in row:
                new_row.append(i)
            cleaned_rows.append(new_row)
        stringed_rows = [[str(e) for e in row] for row in cleaned_rows]
        lengths = [max(map(len, col)) for col in zip(*stringed_rows)]
        format = "    ".join("{{:{}}}".format(x) for x in lengths)
        table = [format.format(*row) for row in stringed_rows]
        print("\n".join(table))

    ###########################
    # Single-Table Operations #
    ###########################

    # Wolfgang
    def query(self, string):
        """
        Return colums from a table that satisfy the queary

        :param string: a query "field == field" or "field == value"
        :return:  A Relation where each row satisfies the query

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel.query("MajorId == 10").print()
        SId    SName    GradYear    MajorId
        3      max      2022        10     
        1      joe      2021        10
        9      lee      2021        10

        >>>
        """
        query = string.split(" ")
        queried_rows = []
        new_row = namedtuple("Row", self.cols)
        selfcols = [field for field in self.cols]
        fieldBool = True if query[2] in selfcols else False
        for row in self.rows:
            if fieldBool == True:
                if getattr(row, query[0]) == getattr(row, query[2]):
                    queried_rows.append(row)
            else:
                if getattr(row, query[0]) == query[2]:
                    queried_rows.append(row)

        return Relation(
            rows=frozenset(
                new_row(*[getattr(row, field) for field in self.cols])
                for row in queried_rows
            ),
            cols=tuple(self.cols),
        )

    # Adam
    def project(self, cols: list) -> Relation:
        """Returns a new Relation with only the specified columns

        :param cols: a list of columns to project
        :return: a Relation containing only the projected columns

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel.project(["SId", "SName"]).print()
        SId    SName
        1      joe
        7      art
        3      max
        5      bob
        8      pat
        2      amy
        9      lee
        6      kim
        4      sue
        """
        new_row = namedtuple("Row", cols)
        return Relation(
            rows=frozenset(
                new_row(*[getattr(row, field) for field in cols]) for row in self.rows
            ),
            cols=tuple(cols),
        )

    # Wolfgang
    def rename(self, old, new) -> Relation:
        """
        Return a relation where the given feild name is changed to the given new name

        :param old: the original field
        :param new: The new field name
        :return:  A Relation identical to the original relation, except for the name change

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel.rename("SName", "StudentName").print()
        SId    StudentName    GradYear    MajorId
        9      lee            2021        10
        2      amy            2020        20
        7      art            2021        30
        6      kim            2020        20
        4      sue            2022        20
        8      pat            2019        20
        1      joe            2021        10
        5      bob            2020        30
        3      max            2022        10

        >>>
        """
        new_cols = [new if field == old else field for field in self.cols]
        new_row = namedtuple("Row", new_cols)
        return Relation(
            rows=frozenset(
                new_row(
                    *[
                        getattr(row, old) if field == new else getattr(row, field)
                        for field in new_cols
                    ]
                )
                for row in self.rows
            ),
            cols=tuple(new_cols),
        )

    # Sam
    def extend(self, name, formula) -> Relation:
        """Create a new attribute by combining or modifying one or more existing attributes

        :param name:  Name of the new column to create
        :param formula:  An expression involving one or more other attributes
        :return:

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel.extend("YearsAfterGraduating", "2023 - int(getattr(row, 'GradYear'))").project(["SId", "SName", "GradYear", "YearsAfterGraduating"]).print()
        SId    SName    GradYear    YearsAfterGraduating
        3       max       2022       1
        7       art       2021       2
        5       bob       2020       3
        2       amy       2020       3
        1       joe       2021       2
        4       sue       2022       1
        8       pat       2019       4
        9       lee       2021       2
        6       kim       2020       3

        >>>
        """
        new_cols = self.cols + (name,)
        new_row = namedtuple("Row", new_cols)
        evaluated_rows = []

        for row in self.rows:
            # Evaluate the given formula
            computed_value = eval(formula)

            # Creates a new row instance with the extended attribute called computed_value
            new_row_instance = new_row(
                *(list(getattr(row, col) for col in self.cols) + [str(computed_value)])
            )
            evaluated_rows.append(new_row_instance)

        return Relation(rows=frozenset(evaluated_rows), cols=new_cols)

    # Adam
    def select(self, operator, *operands) -> Relation:
        """return a new relation with rows matching the operator condition
        :param operator: a function taking the operands as values for the query
        :param operands: operands to be passed to the operator function to perform a query
        :return: a new relation

        :Example:
        >>> import operator
        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel.select(operator.eq, "MajorId", "10").print()
        SId    SName    GradYear    MajorId
        9      lee      2021        10
        1      joe      2021        10
        3      max      2022        10
        """
        result = set()
        for row in self.rows:
            # Takes 'operands' and produces a list where elements that match a field in self.cols
            # are replaced with their value in the original relation
            evaluated_fields = [
                getattr(row, rand) if rand in self.cols else rand for rand in operands
            ]
            if operator(*evaluated_fields):
                result.add(row)
        return Relation(rows=frozenset(result), cols=self.cols)

    # Wolfgang
    def sort(self, cols, order) -> Relation:
        """
        Return a relation with the row order sorted based on the cols and order params

        :param cols: the fields, in order, to sort the rows by
        :param order: a boolean to choose regular or reverse sorting
        :return:  A Relation the same size as the original with the same data, but with a diffrent row order

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel.sort(["GradYear","MajorId"],True).print()
        SId    SName    GradYear    MajorId
        4      sue      2022        20
        3      max      2022        10
        7      art      2021        30
        9      lee      2021        10
        1      joe      2021        10
        5      bob      2020        30
        6      kim      2020        20
        2      amy      2020        20
        8      pat      2019        20

        >>>
        """
        new_row = namedtuple("Row", self.cols)
        cols.reverse()
        sorted_rows = self.rows
        for field in cols:
            sorted_rows = sorted(sorted_rows, key=attrgetter(field), reverse=order)
        return Relation(
            rows=tuple(
                new_row(*[getattr(row, field) for field in self.cols])
                for row in sorted_rows
            ),
            cols=self.cols,
        )

    # Sam
    def groupby(self, cols) -> GroupWrap:
        """Collapse a relation containing one row per unique value in the given group by attributes.

        The groupby operator is always used in conjunction with an aggregate operator.

        * count
        * sum
        * mean
        * median
        * min
        * max

        :param cols: A list of columns to group on
        :return: A GroupWrap object for one of the aggregate operators to work on.


        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> groupBy = (rel.groupby(["GradYear"]).count("SName").project(["GradYear", "count_SName"]).print())
        GradYear    count_SName
        2021       3
        2020       3
        2022       2
        2019       1

        >>>
        """
        new_row = namedtuple("Row", self.cols)

        all = Relation(
            rows=frozenset(
                new_row(*[getattr(row, field) for field in self.cols])
                for row in self.rows
            ),
            cols=tuple(self.cols),
        )

        return GroupWrap(all, cols)

    ###########################
    # Multi-Table Operations #
    ###########################

    # Adam
    def product(self, other) -> Relation:
        """returns a relation which is the cartesian product of self and other

        :param other: another Relation
        :return: a new Relation

        :Example:

        >>> student = Relation(filepath="data/STUDENT.csv")
        >>> enroll = Relation(filepath="data/ENROLL.csv")
        >>> prod = student.product(enroll)
        >>> prod.head(5).print()
        SId    EId
        9      34
        6      24
        3      24
        8      24
        6      14

        """
        new_cols = self.cols + other.cols
        new_row = namedtuple("Row", new_cols)
        args = [self.rows, other.rows]
        pools = [tuple(pool) for pool in args]
        result = [[]]
        for pool in pools:
            result = [x + [y] for x in result for y in pool]
        return Relation(
            rows=frozenset(
                new_row(*[i for entry in prod for i in entry]) for prod in result
            ),
            cols=new_cols,
        )

    # Wolfgang
    def union(self, other) -> Relation:
        """returns a relation which is two relations with identical feilds put top to bottom

        :param other: another Relation
        :return: a new Relation

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel2 = Relation(filepath="data/ENROLL.csv")
        >>> student1 = rel.query("MajorId == 10").print()
        >>> student2 = rel.query("MajorId == 20").print()
        >>> rel.print()
        >>> rel.query('MajorId == 10').union(rel.query('MajorId == 20')).print()
        SId    SName    GradYear    MajorId
        1      joe      2021        10
        9      lee      2021        10
        2      amy      2020        20
        6      kim      2020        20
        4      sue      2022        20
        8      pat      2019        20
        3      max      2022        10

        """
        selfcols = self.cols
        if selfcols == other.cols:
            new_row = namedtuple("Row", selfcols)
            return_rows = []
            for row in self.rows:
                return_rows.append(
                    new_row(*[getattr(row, field) for field in selfcols])
                )
            for row in other.rows:
                return_rows.append(
                    new_row(*[getattr(row, field) for field in other.cols])
                )
            return Relation(rows=tuple(return_rows), cols=tuple(selfcols))
        else:
            raise ValueError("Relations must be union compatible")

    # Sam
    def join(self, other, condition) -> Relation:
        """Create a new relation that is the joining of the two given relations

        :param other:  The relation to compute the join with.
        :param condition: The condition on which to join the tables on
        :return:

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel2 = Relation(filepath="data/ENROLL.csv")
        >>> rel.join(rel2, "SId == StudentId").project(["SName", "GradYear", "MajorId", "EId", "SectionId", "Grade"]).print()
        SName    GradYear    MajorId    EId    SectionId    Grade
        joe       2021       10       14       13           A
        sue       2022       20       44       33           B
        amy       2020       20       34       43           B+
        kim       2020       20       64       53           A
        joe       2021       10       24       43           C
        sue       2022       20       54       53           A
        >>>
        """
        condition = condition.split(" ")
        condition_string = f"row.{condition[0]} {condition[1]} row2.{condition[2]}"
        new_cols = [
            col for col in self.cols + other.cols if col not in condition_string
        ]
        new_rows = namedtuple("Rows", new_cols)
        evaluated_rows = []

        for row in self.rows:
            for row2 in other.rows:
                if eval(condition_string):
                    new_row_instance = new_rows(
                        *(
                            list(
                                getattr(row, col)
                                for col in self.cols
                                if col not in condition_string
                            )
                            + list(
                                getattr(row2, col)
                                for col in other.cols
                                if col not in condition_string
                            )
                        )
                    )
                    evaluated_rows.append(new_row_instance)

        return Relation(rows=frozenset(evaluated_rows), cols=new_cols)

    # Adam
    def semijoin(self, other, condition) -> Relation:
        """return a new relation with records in self matched to elements in the other table on a condition

        :param other: another relation
        :param condition: a condition string
        :return: a new relation

        :Example:

        >>> student = Relation(filepath="data/STUDENT.csv")
        >>> enroll = Relation(filepath="data/ENROLL.csv")
        >>> student.semijoin(enroll, "SId == StudentId").print()
        SId    SName    GradYear    MajorId
        6      kim      2020        20
        4      sue      2022        20
        1      joe      2021        10
        2      amy      2020        20

        """
        condition = condition.split(" ")
        condition_string = f"row.{condition[0]} {condition[1]} row2.{condition[2]}"
        evaluated_rows = []
        for row in self.rows:
            for row2 in other.rows:
                if eval(condition_string):
                    evaluated_rows.append(row)
        return Relation(rows=frozenset(evaluated_rows), cols=self.cols)

    # Wolfgang
    def antijoin(self, other, condition) -> Relation:
        """returns a relation containing the rows from the first relation which do not satisfy the condition

        :param other: another Relation
        :param condition: a condition which rows must not satisfy to be selected
        :return: a new Relation

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel.antijoin(rel.query("MajorId == 10").rename("SId","SId2").rename("SName","SName2").rename("GradeYear","GradeYear2").rename("MajorId","MajorId2"), "MajorId == MajorId2").print()
        SId    SName    GradYear    MajorId
        5      bob      2020        30
        2      amy      2020        20
        4      sue      2022        20
        7      art      2021        30
        6      kim      2020        20
        8      pat      2019        20

        """
        cond = condition.split(" ")
        fieldBool = True if cond[2] in self.cols else False
        print(fieldBool)
        new_rows = []
        new_row = namedtuple("Row", self.cols)
        for row in self.rows:
            check = getattr(row, cond[0])
            matchBool = False
            for row2 in other.rows:
                if check == getattr(row2, cond[2]):
                    matchBool = True
                    break

            if matchBool != True:
                new_rows.append(new_row(*[getattr(row, field) for field in self.cols]))

        return Relation(rows=frozenset(new_rows), cols=tuple(self.cols))

    # Sam
    def outerjoin(self, other, condition) -> "Relation":
        """Create a new relation that is the outerjoin of the two given relations

        :param other:  The relation to compute the outerjoin with.
        :param condition: The condition on which to join the tables on
        :return:

        :Example:

        >>> rel = Relation(filepath="data/STUDENT.csv")
        >>> rel2 = Relation(filepath="data/ENROLL.csv")
        >>> rel.outerjoin(rel2, "SId == StudentId").project(["SName", "GradYear", "MajorId", "EId", "SectionId", "Grade"]).print()
        SId    SName    GradYear    MajorId    EId    StudentId    SectionId    Grade
        5       bob       2020       30       NULL       NULL       NULL       NULL
        1       joe       2021       10       14       1       13       A
        9       lee       2021       10       NULL       NULL       NULL       NULL
        3       max       2022       10       NULL       NULL       NULL       NULL
        7       art       2021       30       NULL       NULL       NULL       NULL
        8       pat       2019       20       NULL       NULL       NULL       NULL
        1       joe       2021       10       24       1       43       C
        2       amy       2020       20       34       2       43       B+
        6       kim       2020       20       64       6       53       A
        4       sue       2022       20       54       4       53       A
        4       sue       2022       20       44       4       33       B
        >>>
        """
        condition = condition.split(" ")
        condition_string = f"row.{condition[0]} {condition[1]} row2.{condition[2]}"
        new_cols = self.cols + other.cols
        new_rows = namedtuple("Rows", new_cols)
        evaluated_rows = []

        for row in self.rows:
            matched = False
            for row2 in other.rows:
                if eval(condition_string):
                    matched = True
                    new_row_instance = new_rows(
                        *(
                            list(getattr(row, col) for col in self.cols)
                            + list(getattr(row2, col) for col in other.cols)
                        )
                    )
                    evaluated_rows.append(new_row_instance)

            # If no match is found in other.rows, include row with null values for other.cols
            if not matched:
                new_row_instance = new_rows(
                    *(
                        list(getattr(row, col) for col in self.cols)
                        + ["NULL"] * len(other.cols)
                    )
                )
                evaluated_rows.append(new_row_instance)

        return Relation(rows=frozenset(evaluated_rows), cols=new_cols)

    def head(self, n: int = 10):
        if n > len(self.rows):
            n = len(self.rows)
        return Relation(rows=frozenset(list(self.rows)[:n]), cols=self.cols)

    def __repr__(self):
        return str(self.rows)


class TestRelation:
    @pytest.fixture
    def course(self):
        return Relation(filepath="data/COURSE.csv")

    @pytest.fixture
    def dept(self):
        return Relation(filepath="data/DEPT.csv")

    @pytest.fixture
    def enroll(self):
        return Relation(filepath="data/ENROLL.csv")

    @pytest.fixture
    def section(self):
        return Relation(filepath="data/SECTION.csv")

    @pytest.fixture
    def student(self):
        return Relation(filepath="data/STUDENT.csv")

    ###########################
    # Single-Table Operations #
    ###########################

    def test_project(self, course, dept, enroll, section, student):
        rels = [course, dept, enroll, section, student]
        for r in rels:
            current_cols = r.cols
            proj_cols = [current_cols[0]]
            assert list(r.project(proj_cols).cols) == proj_cols

    def test_rename(self, student):
        rename_method = student.rename("SName","StudentName")
        assert list(rename_method.cols) == ["SId","StudentName","GradYear","MajorId"]

    def test_extend(self, student):
        formula = "2023 - int(getattr(row, 'GradYear'))"
        new_row = "YearsAfterGraduating"
        extend_method = student.extend(new_row, formula)

        for row in extend_method.rows:
            assert hasattr(row, new_row)

    def test_select(self, student):
        import operator

        match = 0
        for row in student.rows:
            if row.MajorId == "10":
                match += 1
        select = student.select(operator.eq, "MajorId", "10")
        assert len(select.rows) == match
        for row in select.rows:
            assert row.MajorId == "10"

    def test_sort(self, student):
        ascending = True
        sort_cols = ["GradYear"]
        sort_method = student.sort(sort_cols,ascending)
        first = True
        last_row = ""
        for row in sort_method.rows:
            if first == True:
                last_row = row
                first = False
            else:
                assert getattr(last_row, "GradYear") >= getattr(row, "GradYear")
                last_row = row

    def test_groupby(self, student):
        cols = ["GradYear"]
        new_col = "count_SName"
        groupby_method = student.groupby(cols).count("SName")

        for row in groupby_method.rows:
            assert hasattr(row, new_col)

    ###########################
    # Multi-Table Operations #
    ###########################

    def test_product(self, student, enroll):
        from itertools import product

        assert len([x for x in product(student.rows, enroll.rows)]) == len(
            student.product(enroll).rows
        )

    def test_union(self, student):
        relation_one = student.query("MajorId == 30")
        relation_two = student.query("MajorId == 10")
        union_method = relation_one.union(relation_two)
        union_method.print()
        count = 0
        for row in union_method.rows:
            if count < len(relation_one.rows):
                assert getattr(row, "MajorId") == "30"
            else:
                assert getattr(row, "MajorId") == "10"
            count = count + 1

    def test_join(self, student, enroll):
        cols = ["SName", "GradYear", "MajorId", "EId", "SectionId", "Grade"]
        condition = "SId == StudentId"
        join_method = student.join(enroll, condition)

        for col in cols:
            assert any(hasattr(row, col) for row in join_method.rows)

    def test_semijoin(self, student, enroll):
        student_ids = [row.StudentId for row in enroll.rows]
        enroll_rows_with_student_id = set()
        for row in student.rows:
            if row.SId in student_ids:
                enroll_rows_with_student_id.add(row)
        assert len(enroll_rows_with_student_id) == len(
            student.semijoin(enroll, "SId == StudentId").rows
        )

    def test_antijoin(self, student):
        condition = "MajorId == MajorId2"
        relation_one = student
        relation_two = student.query("MajorId == 30").rename("SId","SId2").rename("SName","SName2").rename("GradYear","GradYear2").rename("MajorId","MajorId2")
        antijoin_method = relation_one.antijoin(relation_two, condition)
        for row in antijoin_method.rows:
            assert getattr(row, "MajorId") != "30"

    def test_outerjoin(self, student, enroll):
        cols = [
            "SId",
            "SName",
            "GradYear",
            "MajorId",
            "EId",
            "StudentId",
            "SectionId",
            "Grade",
        ]
        condition = "SId == StudentId"
        outerjoin_method = student.outerjoin(enroll, condition)

        for col in cols:
            assert any(hasattr(row, col) for row in outerjoin_method.rows)


rel = Relation(filepath="data/STUDENT.csv")
rel2 = Relation(filepath="data/ENROLL.csv")
rel3 = Relation(filepath="data/SECTION.csv")
rel4 = Relation(filepath="data/COURSE.csv")


# rel = Relation(filepath="data/STUDENT.csv")
# student = Relation(filepath="data/STUDENT.csv")
# enroll = Relation(filepath="data/ENROLL.csv")
# prod = student.project(["SId"]).product(enroll.project(["EId"]))

# rel.project(["SId", "SName", "GradYear"]).print()

# Extend Testing
# rel.extend("YearsAfterGraduating", "2023 - int(getattr(row, 'GradYear'))").project(["SId", "SName", "GradYear", "YearsAfterGraduating"]).print()

# Groupby Testing
# groupBy = (
#     rel.groupby(["GradYear"])
#     .count("SName")
#     .project(["GradYear", "count_SName"])
#     .print()
# )
# groupBy = (
#     rel.groupby(["GradYear"])
#     .sum("MajorId")
#     .project(["GradYear", "sum_MajorId"])
#     .print()
# )
# groupBy = (
#     rel.groupby(["GradYear"])
#     .max("MajorId")
#     .project(["GradYear", "max_MajorId"])
#     .print()
# )
# groupBy = (
#     rel.groupby(["GradYear"])
#     .min("MajorId")
#     .project(["GradYear", "min_MajorId"])
#     .print()
# )
# groupBy = (
#     rel.groupby(["MajorId"])
#     .mean("GradYear")
#     .project(["MajorId", "mean_GradYear"])
#     .print()
# )
# groupBy = (
#     rel.groupby(["GradYear"])
#     .median("MajorId")
#     .project(["GradYear", "median_MajorId"])
#     .print()
# )

# rename and sort testing
# rel.rename("SName", "StudentName").print()
# rel.sort(["GradYear","MajorId"],True).print()

# Join testing
rel.join(rel2, "SId == StudentId").project(
    ["SName", "GradYear", "MajorId", "EId", "SectionId", "Grade"]
).print()  # Formatting is kind of funny here
rel3.join(rel4, "CourseId == CId").print() # Formatting is kind of funny here

# Outer Join testing
# outerjoin = rel.outerjoin(rel2, "SId == StudentId")
# outerjoin.project(
#     ["SId", "SName", "GradYear", "MajorId", "EId", "StudentId", "SectionId", "Grade"]
# ).print()
# outerjoin2 = rel3.outerjoin(rel4, "CourseId == CId")
# outerjoin2.project(
#     ["SectId", "CourseId", "Prof", "YearOffered", "CId", "Title", "DeptId"]
# ).print()

# Union testing
# rel = Relation(filepath="data/STUDENT.csv")
#rel2 = Relation(filepath="data/STUDENT.csv")
# rel2 = Relation(filepath="data/ENROLL.csv")
# student1 = rel.query("MajorId == 10").print()
# student2 = rel.query("MajorId == 20").print()
# rel.print()
# rel.query('MajorId == 10').union(rel.query('MajorId == 20')).print()

# Antijoin testing
#rel.join(rel2, "SId == StudentId").print()
#rel.antijoin(rel2, "SId == StudentId").print()
#rel.project(["SId","SName","GradYear","MajorId"]).print()
#rel.query("MajorId == 10").print()
# second = rel.query("MajorId == 10").rename("SId","SId2").rename("SName","SName2").rename("GradeYear","GradeYear2").rename("MajorId","MajorId2").print()
# rel.antijoin(rel.query("MajorId == 10").rename("SId","SId2").rename("SName","SName2").rename("GradeYear","GradeYear2").rename("MajorId","MajorId2"), "MajorId == MajorId2").print()