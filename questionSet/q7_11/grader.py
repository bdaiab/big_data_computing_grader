from grader_support import gradelib

grader = gradelib.Grader()

# execute a raw student code & capture stdout
# grader.add_preprocessor(gradelib.wrap_in_string)
grader.set_isStream(True)
grader.add_test(gradelib.ExecWrappedStudentCodeTest({}, "Compare the output of your program with the sample solution."))
