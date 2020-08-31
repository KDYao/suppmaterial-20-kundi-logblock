"""
This script takes generated log blocks as input and test LogArchive compression on these blocks
At first stage, we only implement the compression ratio without considering the speed
"""
from datetime import datetime
import subprocess
import os
import shutil


class RunLogArchive:

    def __init__(self, logpath: str, out_dir: str, jhis=10, bucks=8):
        """
        Init
        :param logpath: log to be compressed
        :param out_dir: write to which directoruy
        """
        self.logpath = os.path.abspath(logpath)
        current_file_path = os.path.dirname(os.path.abspath(__file__))
        self.logarchive_path = os.path.join(os.path.dirname(current_file_path),
                                            os.path.sep.join(['log_archive_v0', 'bin', 'Release', 'Archiver']))
        self.jhis = jhis
        self.bucks = bucks
        self.out_dir = out_dir
        if os.path.isdir(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir)

    def log_archive_compress(self):

        """
        Compress with LogArchive
        :return:
        """

        # Create model
        logarchive_compress_start_time = datetime.now()

        compress_cmd = 'cat {log_file} | {log_archive_path} -c'.format(
            log_file=self.logpath,
            log_archive_path=self.logarchive_path
        )

        # Set output dir as cwd, where logarchive output stays
        subprocess.Popen(compress_cmd, shell=True, cwd=self.out_dir).wait()


