#!/usr/bin/python3
import os, sys, subprocess

from multiprocessing import cpu_count
from itertools import chain, combinations

def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

oversub_deg = 2
max_pes = oversub_deg * cpu_count()
success_threshold = 0.5
reliability_threshold = 0.98

pes = range(1, max_pes + 1)

def get_factors(x):
    for i in range(1, x + 1):
        if x % i == 0:
            yield(i)

def run_test(cmd, timeout):
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        outs, errs = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
    exit_code = proc.wait()
    return None if (exit_code == 0) else (outs, errs, exit_code)

transients=[]
def retry(cmd, timeout, num_retries):
    successes = 0
    for _ in range(num_retries):
        res = run_test(cmd, timeout)
        successes += (1 if res is None else 0)
    return successes / num_retries

def run_tests(name, timeout=10, num_retries=4):
    failures = []
    for p in pes:
        for ppn in get_factors(p):
            cmd = [ "./charmrun", "++local", "+p" + str(p), "++ppn", str(ppn), name, "+setcpuaffinity", "+CmiSleepOnIdle" ]
            res = run_test(cmd, timeout)
            if res is not None:
                rate = retry(cmd, timeout, num_retries)
                if rate == 0:
                    failures.append((p, ppn))
                    print("\n[FAILURE] test failed:", *cmd)
                else:
                    transients.append((p, ppn, rate))
                    print("\n[WARNING] transient failure in (retry rate %f%%):" % (rate,), *cmd)
                print(res)
            else:
                print("\n[SUCCESS] test passed:", *cmd)
    return failures

def build_and_run(cwd, name, opts):
    os.chdir(cwd)
    os.environ["OPTS"] = " ".join(opts)
    if (not os.system("make clean")) and (not os.system("make")):
        return run_tests(name)
    else:
        return [("make",)]

pwd = os.path.dirname(os.path.realpath(__file__))
all_opts = [ list(x) for x in powerset([ \
    "-DDIRECT_ROUTE", "-DNODE_LEVEL", "-DDIRECT_BUFFER", "-DINLINE_SEND", "-DRANDOMIZE_SENDS" ]) ]
all_opts = [ [ "-DHYPERCOMM_TRACING_ON" ] ] + all_opts
num_tests = len(all_opts) * sum( len(list(get_factors(p))) for p in pes )

all_failures = []
for opts in all_opts:
    transceivers = os.path.join(pwd, "transceivers")
    print("filename:", transceivers)
    failures = build_and_run(transceivers, "hello", opts)
    all_failures.extend((opts, *x) for x in failures)

reliability = (num_tests - len(transients)) / num_tests
if len(all_failures) != 0:
    print("\n[FAILING] The following tests failed:\n%s" % (all_failures,))
    exit_code = -1
else:
    print("\n[SUCCESS] All %d tests passed (eventually) (%f%% reliability)." % (num_tests, 100 * reliability))
    exit_code = 0

if len(transients) != 0:
    print("\n[WARNING] Transient failures in the following cases:")
    for transient in transients:
        print("Test %s, %f%% of the retries were successful" % (transient[:-1], transient[-1] * 100))
        if transient[-1] <= success_threshold:
            exit_code = -1

if reliability < reliability_threshold:
    exit_code = -1

sys.exit(exit_code)
