"""Tests for the sleep guard in run_eval.py (standard library only):  python3 eval/test_run_eval.py"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_eval as r


class Clock:
    """A fake wall clock and uptime clock. A call to fn advances both; a 'sleeping' call advances only the wall clock."""
    def __init__(self, plan):
        self.plan, self.wall, self.up, self.calls = list(plan), 0.0, 0.0, 0

    def fn(self):
        slept = self.plan.pop(0)
        self.calls += 1
        self.wall += 30 + slept
        self.up += 30
        return "result-%d" % self.calls


class Guard(unittest.TestCase):
    def run_guard(self, plan, tries=3):
        c = Clock(plan)
        return c, r.guarded(c.fn, tries, clock=lambda: c.wall, awake=lambda: c.up)

    def test_a_run_without_sleep_is_kept(self):
        c, out = self.run_guard([0])
        self.assertEqual((out, c.calls), ("result-1", 1))

    def test_a_run_that_overlaps_a_sleep_is_repeated(self):
        c, out = self.run_guard([2000, 0])           # first run spans a 2000 s sleep, second is clean
        self.assertEqual((out, c.calls), ("result-2", 2))

    def test_gives_up_after_the_last_try(self):
        with self.assertRaisesRegex(RuntimeError, "kept sleeping"):
            self.run_guard([100, 100, 100])

    def test_a_small_clock_wobble_is_not_a_sleep(self):
        c, out = self.run_guard([r.SLEPT_LIMIT_S - 1])
        self.assertEqual(c.calls, 1)

    def test_a_real_failure_is_raised_not_retried_away(self):
        def boom():
            raise ValueError("replay failed")
        with self.assertRaisesRegex(ValueError, "replay failed"):
            r.guarded(boom, 3, clock=lambda: 0.0, awake=lambda: 0.0)

    def test_dc_turns_every_argument_into_a_string(self):
        # regression: a float speed reached subprocess and the failure was hidden by a swallowed exception
        import subprocess
        seen = {}
        orig = subprocess.run
        subprocess.run = lambda cmd, **k: seen.update(cmd=cmd) or type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
        try:
            r.dc("run", "--speed", 6.0, "--rate", 0)
        finally:
            subprocess.run = orig
        self.assertTrue(all(isinstance(x, str) for x in seen["cmd"]), seen["cmd"])


if __name__ == "__main__":
    unittest.main(verbosity=1)
