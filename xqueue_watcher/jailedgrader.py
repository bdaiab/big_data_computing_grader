"""
An implementation of a grader that uses codejail to sandbox submission execution.
"""
import codecs
import os
import sys
import imp
import json
import random
import gettext
from path import Path
import six
import os.path
import codejail
import platform
from subprocess import Popen, PIPE
from pyspark.find_spark_home import _find_spark_home
from codejail.subproc import run_subprocess
from grader_support.gradelib import EndTest
from grader_support.graderutil import LANGUAGE
import grader_support
import traceback
from .grader import Grader

TIMEOUT = 1
master_args = " --master spark://`hostname`:7077 "
class JailResult:
    """
    A passive object for us to return from jail_code.
    """
    def __init__(self):
        self.stdout = self.stderr = self.status = None

def path_to_six():
    """
    Return the full path to six.py
    """
    if any(six.__file__.endswith(suffix) for suffix in ('.pyc', '.pyo')):
        # __file__ points to the compiled bytecode in python 2
        return Path(six.__file__[:-1])
    else:
        # __file__ points to the .py file in python 3
        return Path(six.__file__)


SUPPORT_FILES = [
    Path(grader_support.__file__).dirname(),
    path_to_six(),
]


def truncate(out):
    """
    Truncate test output that's too long.  This is per-test.
    """
    TOO_LONG = 5000    # 5K bytes seems like enough for a single test.
    if len(out) > TOO_LONG:
        out = out[:TOO_LONG] + "...OUTPUT TRUNCATED"

    return out


def prepend_coding(code):
    """
    Add a coding line--makes submissions with inline unicode not
    explode (as long as they're utf8, I guess)
    """
    return '# coding: utf8\n' + code


class JailedGrader(Grader):
    """
    A grader implementation that uses codejail.
    Instantiate it with grader_root="path/to/graders"
    and optionally codejail_python="python name" (the name that you used to configure codejail)
    """
    def __init__(self, *args, **kwargs):
        self.codejail_python = kwargs.pop("codejail_python", "python")
        super().__init__(*args, **kwargs)
        self.locale_dir = self.grader_root / "conf" / "locale"
        self.fork_per_item = False  # it's probably safe not to fork
        # EDUCATOR-3368: OpenBLAS library is allowed to allocate 1 thread
        os.environ["OPENBLAS_NUM_THREADS"] = "1"

    def _enable_i18n(self, language):
        trans = gettext.translation('graders', localedir=self.locale_dir, fallback=True, languages=[language])
        trans.install(names=None)

    def _run(self, grader_path, thecode, seed, timeout):
        files = SUPPORT_FILES + [grader_path]
        if self.locale_dir.exists():
            files.append(self.locale_dir)

        extra_files = [('submission.py', thecode.encode('utf-8'))]
        argv = ["-m", "grader_support.run", Path(grader_path).basename(), 'submission.py', seed]
        # r = codejail.jail_code.jail_code(self.codejail_python, files=files, extra_files=extra_files, argv=argv)
        # self.log.debug(f'config: {self.codejail_python}\nin _run: {r.stdout}\n error: {r.stderr}\n')
        rm_cmd = []
        on_windows = platform.system() == "Windows"
        # SPARK_HOME = _find_spark_home()
        script = "./bin/spark-submit.cmd" if on_windows else "./bin/spark-submit"
        SPARK_HOME = os.getenv("SPARK_HOME")
        command = os.path.join(SPARK_HOME, script)
        command = command + master_args
        with codejail.util.temp_directory() as homedir:
            # Make directory readable by other users ('sandbox' user needs to be
            # able to read it).
            os.chmod(homedir, 0o775)

            # Make a subdir to use for temp files, world-writable so that the
            # sandbox user can write to it.
            tmptmp = os.path.join(homedir, "tmp")
            os.mkdir(tmptmp)
            os.chmod(tmptmp, 0o777)

            for name, content in extra_files or ():
                with open(os.path.join(homedir, name), "wb") as extra:
                    extra.write(content)
                # with open(os.path.join(homedir, name), 'r', encoding='utf-8') as f:
                #     content = f.read()
                #     self.log.debug(f'submission: {content}')
                command = command + os.path.join(homedir, name)

            # self.log.debug(f'command: {command}\n')
            result = JailResult()
            proc = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
            output, err = proc.communicate(timeout=timeout)
            # self.log.debug(f'output: {output}\nerror: {err}\n')

            try:
                test = eval(str(output.decode('utf-8').replace('\n', '')))
            except Exception as e:
                output = str([output.decode('utf-8').replace('\n', '')]).encode('utf-8')
                # self.log.error(traceback.format_exc())

            result.status = proc.wait()
            result.stdout = output
            result.stderr = err

            # Remove the tmptmp directory as the sandbox user since the sandbox
            # user may have written files that the application user can't delete.
            rm_cmd.extend([
                '/usr/bin/find', tmptmp,
                '-mindepth', '1', '-maxdepth', '1',
                '-exec', 'rm', '-rf', '{}', ';'
            ])

            # Run the rm command subprocess.
            run_subprocess(rm_cmd, cwd=homedir)

        return result

    def grade(self, grader_path, grader_config, submission, student_info):
        if type(submission) != str:
            self.log.warning("Submission is NOT unicode")

        results = {
            'errors': [],
            'tests': [],
            'correct': False,
            'score': 0,
        }
        self.log.info(f"Start grading submission from {student_info['anonymous_student_id']}, submission time: {student_info['submission_time']}.")

        # There are some cases where the course team would like to accept a
        # student submission but not process the student code. Some examples are
        # cases where the problem would require dependencies that are difficult
        # or impractical to install in a sandbox or if the complexity of the
        # solution would cause the runtime of the student code to exceed what is
        # possible in the sandbox.

        # skip_grader is a flag in the grader config which is a boolean. If it
        # is set to true on a problem then it will always show that the
        # submission is correct and give the student a full score for the
        # problem.
        if grader_config.get('skip_grader', False):
            results['correct'] = True
            results['score'] = 1
            self.log.debug('Skipping the grader.')
            return results

        self._enable_i18n(grader_config.get("lang", LANGUAGE))

        # load answer
        answer_path = Path(grader_path).dirname() / 'ans'
        with open(answer_path, 'rb') as f:
            answer = f.read().decode('utf-8')

        # Import the grader, straight from the original file.  (It probably isn't in
        # sys.path, and we may be in a long running gunicorn process, so we don't
        # want to add stuff to sys.path either.)
        grader_module = imp.load_source("grader_module", str(grader_path))
        grader = grader_module.grader
        isStream = grader.get_isStream()
        timeout = 75 if isStream else 45
        # self.log.debug(f'isStream: {isStream}\n')

        # Preprocess for grader-specified errors
        errors = grader.input_errors(submission)
        if errors:
            results['errors'].extend(errors)
            # Don't run tests if there were errors
            return results

        # Add a unicode encoding declaration.
        # processed_answer = prepend_coding(grader.preprocess(answer))
        processed_submission = prepend_coding(grader.preprocess(submission))

        # Same seed for both runs
        seed = random.randint(0, 20000)

        expected = eval(answer.replace('\n', ''))


        actual_ok = False
        actual_exc = None
        try:
            # Do NOT trust the student solution (in production).
            actual_outputs = None   # in case run raises an exception.
            actual_outputs = self._run(grader_path, processed_submission, seed, timeout)
            # self.log.debug(f'grader_path: {grader_path}\nprocessed_submission: {processed_submission}\nexpected_output: {actual_outputs.stdout}\n')
            if actual_outputs:
                actual = eval(actual_outputs.stdout.decode('utf-8').replace('\n', ''))
                actual_ok = True
            else:
                results['errors'].append(_("There was a problem running your solution (Staff debug: L379)."))
        except Exception:
            actual_exc = sys.exc_info()
        else:
            if actual_ok or actual_outputs.stderr:
                if actual_outputs.stderr:
                    # The grader ran OK, but the student code didn't, so show the student
                    # details of what went wrong.  There is probably an exception to show.
                    if isinstance(actual_outputs.stderr, bytes):
                        shown_error = actual_outputs.stderr.decode('utf-8')
                    elif isinstance(actual_outputs.stderr, str):
                        shown_error = actual_outputs.stderr
                    else:
                        shown_error = 'There was an error thrown while running your solution.'
                    results['errors'].append(shown_error)
            else:
                # The grader didn't run well, we are going to bail.
                actual_ok = False

        # If something went wrong, then don't continue
        if not actual_ok:
            results['errors'].append(_("Something went wrong with your code(Staff debug: L397).\n"
                                       f"Error Message:\n{actual_outputs.stderr.decode('utf-8')}\n"
                                       f"Your Output:\n{actual_outputs.stdout.decode('utf-8')}"))
            self.log.error("Couldn't run student solution. grader = %s, output: %r",
                           grader_path, actual_outputs, exc_info=actual_exc)
            return results

        try:
            test = len(expected)
        except:
            expected = [expected]
        try:
            test = len(actual)
        except:
            actual = [actual]


        # Compare actual and expected through the grader tests, but only if we haven't
        # already found a problem.
        corrects = []
        expected_results = expected
        actual_results = actual
        # self.log.debug(f'expected_res: {expected_results}-{len(expected_results)}\nactual_res: {actual_results}-{len(actual_results)}')


        if len(expected_results) != len(actual_results):
            if isStream:
                idx = random.randint(0, min(4,len(expected_results)-1,len(actual_results)-1))
                res_expected = expected_results[idx]
                res_actual = actual_results[idx]
                idx_len = 1
            else:
                results['errors'].append(_('Something went wrong: The length of your result is '
                                           'inconsistent with our reference code.\n'
                                           f'Length of your answer: {len(actual_results)}\n'
                                           f'Length of reference output: {len(expected_results)}\n'
                                           f'Your output: {actual_results}'))
                return results

        else:
            if isStream:
                idx = random.randint(0, min(4,len(expected_results)-1,len(actual_results)-1))
                res_expected = expected_results[idx]
                res_actual = actual_results[idx]
                idx_len = 1

            elif(len(expected_results)<=20):
                idx = 0
                idx_len = len(expected_results)
                res_expected = expected
                res_actual = actual

            else:
                idx = seed % (len(expected_results) - 19)
                idx_len = 20
                res_expected = expected[idx:idx + 20]
                res_actual = actual[idx:idx + 20]

        corrects.append(res_expected == res_actual)
        if not grader_config.get("hide_output", False):
            results['tests'].append((grader.tests()[0].short_description, f'Compare the output of your code with our reference code with index {idx} to {idx+idx_len}(not including {idx+idx_len}).',
                                     corrects[0], str(res_expected), str(res_actual)))

        # If there were no tests run, then there was probably an error, so it's incorrect
        n = len(corrects)
        results['correct'] = all(corrects) and n > 0
        results['score'] = float(sum(corrects))/n if n > 0 else 0

        if n == 0 and len(results['errors']) == 0:
            results['errors'] = [
                _("There was a problem while running your code (Staff debug: L450). "
                  "Please contact the course staff for assistance.")
            ]
        self.log.info(f'Done, result of {student_info["anonymous_student_id"]} : {results}')
        return results


def main(args):     # pragma: no cover
    """
    Prints a json list:
    [ ("Test description", "value") ]

    TODO: what about multi-file submission?
    """
    import logging
    from pprint import pprint
    from codejail.jail_code import configure
    import getpass

    logging.basicConfig(level=logging.INFO)
    if len(args) != 2:
        return

    configure("python", sys.executable, user=getpass.getuser())
    (grader_path, submission_path) = args

    with open(submission_path) as f:
        submission = f.read().decode('utf-8')

    grader_config = {"lang": "eo"}
    grader_path = path(grader_path).abspath()
    g = JailedGrader(grader_root=grader_path.dirname().parent.parent)
    pprint(g.grade(grader_path, grader_config, submission))


if __name__ == '__main__':      # pragma: no cover
    main(sys.argv[1:])
