
import unittest
import functools
import io
import os

import cron_scheduler

import mock


class TestCronSchedulerBase(unittest.TestCase):

    TEST_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data/test1.cron')
    TEST_FILE_INITIAL_CONTENTS = ''

    def setUp(self):
        with open(self.TEST_FILE_PATH, 'r') as f:
            self.TEST_FILE_INITIAL_CONTENTS = f.read()

    def tearDown(self):
        with open(self.TEST_FILE_PATH, 'w') as f:
            f.write(self.TEST_FILE_INITIAL_CONTENTS)

    def get_cron_lines(self, contents):
        return filter(lambda x : x,
                      [line.strip() if not line.startswith('#') else ''
                                    for line in contents.split('\n')])


class TestExistAddRemoveInCronScheduler(TestCronSchedulerBase):

    def test_cron_job_exists_finds_existing(self):
        cron_line = self.get_cron_lines(self.TEST_FILE_INITIAL_CONTENTS)[0]
        scheduler = cron_scheduler.CronScheduler(
            cron_file_path=self.TEST_FILE_PATH
        )
        exists = scheduler.cron_job_exists(cron_line)
        assert exists

    def test_add_cron_job_doesnt_add_existing(self):
        scheduler = cron_scheduler.CronScheduler(
            cron_file_path=self.TEST_FILE_PATH
        )
        cron_line = self.get_cron_lines(self.TEST_FILE_INITIAL_CONTENTS)[0]
        scheduler.add_cron_job(cron_line)
        with open(self.TEST_FILE_PATH, 'r') as f:
            lines = self.get_cron_lines(f.read())
            assert len(lines) == 1
            assert lines[0] == cron_line

    def test_add_cron_job_with_new_job_does_get_added(self):
        scheduler = cron_scheduler.CronScheduler(
            cron_file_path=self.TEST_FILE_PATH
        )
        cron_line = self.get_cron_lines(self.TEST_FILE_INITIAL_CONTENTS)[0]
        new_cron_line = cron_line[:-1] + '2'
        scheduler.add_cron_job(new_cron_line)
        with open(self.TEST_FILE_PATH, 'r') as f:
            lines = self.get_cron_lines(f.read())
            assert len(lines) == 2, 'These lines are present: {}'.format(lines)
            assert scheduler.cron_job_exists(cron_line)
            assert scheduler.cron_job_exists(new_cron_line)

    def test_remove_cron_job_when_present_gets_removed(self):
        scheduler = cron_scheduler.CronScheduler(
            cron_file_path=self.TEST_FILE_PATH
        )
        cron_line = self.get_cron_lines(self.TEST_FILE_INITIAL_CONTENTS)[0]
        scheduler.remove_cron_job(cron_line)
        with open(self.TEST_FILE_PATH, 'r') as f:
            lines = self.get_cron_lines(f.read())
            assert len(lines) == 0, 'These lines are present: {}'.format(lines)


class TestSynchronizeInCronScheduler(TestCronSchedulerBase):

    TEST_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data/test2.cron')

    def test_synchronize_cron_jobs_removes_one_job_no_longer_in_db(self):

        # The first cron job was deleted from the database since it was
        # initially added. Now, only the second one remains.
        # With just one left, the old one should be deleted

        cron_line = self.get_cron_lines(self.TEST_FILE_INITIAL_CONTENTS)[1]
        expected_cron_jobs = [cron_line]

        scheduler = cron_scheduler.CronScheduler(
            cron_file_path=self.TEST_FILE_PATH
        )
        scheduler.synchronize_cron_jobs(expected_cron_jobs)

        with open(self.TEST_FILE_PATH, 'r') as f:
             lines = self.get_cron_lines(f.read())
             assert len(lines) == 1, 'These lines are present: {}'.format(lines)
             assert lines[0] == cron_line

    def test_synchronize_cron_jobs_removes_two_jobs_no_longer_in_db(self):

        # Both original cron jobs were deleted from the database since they
        # were initially added. Another new one was also just added, though.

        cron_line = '* * * * * root ./execute --scheduled-query-id=4'
        expected_cron_jobs = [cron_line]

        scheduler = cron_scheduler.CronScheduler(
            cron_file_path=self.TEST_FILE_PATH
        )
        scheduler.synchronize_cron_jobs(expected_cron_jobs)

        with open(self.TEST_FILE_PATH, 'r') as f:
             lines = self.get_cron_lines(f.read())
             assert len(lines) == 1, 'These lines are present: {}'.format(lines)
             assert lines[0] == cron_line
        

class TestCronWithShellVariableDeclarations(TestCronSchedulerBase):
    
    TEST_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data/test3.cron')

    def test_variable_declarations_stay_in_order_at_top_of_file(self):
        initial_lines = self.get_cron_lines(self.TEST_FILE_INITIAL_CONTENTS)
        scheduler = cron_scheduler.CronScheduler(
            cron_file_path=self.TEST_FILE_PATH
        )
        scheduler.add_cron_job('* * * * * root /usr/bin/touch /tmp/test.txt')
        counter = 0 # We're just counting lines that 
                    # contain shell declarations 
        with scheduler._open_cron_file(mode='r') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                # Ignore empty or comment lines;
                # Also, we're only checking the first three 
                # lines we get (the shell variables)
                if not line or line.startswith('#'):
                    continue
                elif counter >= 3:
                    break
                else:
                    assert initial_lines[counter] == line, (
                            "'{}' should be '{}'".format( 
                            line, initial_lines[counter]))    
                    counter += 1
