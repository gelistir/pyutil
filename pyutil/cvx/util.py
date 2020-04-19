import cvxpy as cvx

# I have addressed this here https://github.com/cvxgrp/cvxpy/issues/966


class Solver(object):
    def __init__(self, problem):
        assert isinstance(problem, cvx.Problem)
        self.problem = problem
        self.parameters = {a.name(): a for a in problem.parameters()}
        self.variables = {a.name(): a for a in problem.variables()}

    def solve(self, *args, **kwargs):
        self.problem.solve(*args, **kwargs)
        # Check that the solver computed an optimal solution
        assert self.problem.status == "optimal", "Couldn't find an optimal solution. Status {s}".format(
            s=self.problem.status)
