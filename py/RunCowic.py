"""
This script takes generated log blocks as input and test cowic compression on these blocks
At first stage, we only implement the compression ratio without considering the speed
"""
from datetime import datetime
import subprocess
import os
import shutil


class RunCowic:

    def __init__(self, logpath, out_dir):
        """
        Init
        :param logpath: log to be compressed
        :param out_dir: write to which directoruy
        """
        self.logpath = logpath
        self.model_path = os.path.join(out_dir, os.path.basename(logpath).rstrip('.log') + '.mdl')
        self.out_dir = out_dir
        if os.path.isdir(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir)



    def truncate_log_for_training(self, file_path):
        """
        [DEPRECATED]
        Truncate a log subset for training
        :param file_path: The base file to truncate from
        :return: trunc_log_path
        """
        trunc_log_path = os.path.join(self.model_dir, os.path.basename(file_path).split('.')[0] + '_5p.log')

        if self.isRandom or not os.path.isfile(trunc_log_path):
            # Generate a truncate of log
            truncate_cmd = 'head -c {training_size} {file_path} > {output_file_path}'.format(
                training_size=str(self.training_size),
                file_path=file_path,
                output_file_path=trunc_log_path)
            subprocess.Popen(truncate_cmd, shell=True).wait()
        return trunc_log_path

    def pre_train_model(self):
        """
        Pre-train a model for log file and save it
        :param model_training_log_path: The log file to train a model
        :return:
        """
        # Create model from truncate log

        cowic_training_cmd = 'bin/compressor_cmd_tool -t {log_path} -m {log_model}'.format(
            log_path=self.logpath, log_model=self.model_path
        )

        subprocess.Popen(cowic_training_cmd, shell=True, cwd='../cowic').wait()

        if not os.path.isfile(self.model_path):
            raise FileNotFoundError('Model %s not created' % self.model_path)

    @staticmethod
    def change_config(filetype: str):
        """
        This function changes the configuration file according to current log type
        :param: filetype: type of log file
        :return:
        """
        # Init config
        shutil.copyfile('../cowic/backup/config.ini', '../cowic/config.ini')

        if filetype == 'HealthApp': return

        # Rewrite config
        with open('../cowic/config.ini', 'r+') as corw:
            lines = corw.read().splitlines()
            idx = lines.index(';%s' % filetype)
            if not lines[idx+2].startswith(';SpecifyColumnModel'):
                raise ValueError('[Error] Check config file: SpecifyColumnModel not found '
                                 'in dataset: {filetype}; line: {line}'.format(
                    filetype=filetype, line=lines[idx+2].startswith(';SpecifyColumnModel')
                ))
            lines[idx+2] = lines[idx+2][1:]
            corw.write('\n'.join(lines))
        corw.close()

    def cowic_compress(self):

        """
        Compress with Cowic
        :return:
        """

        # Create model
        cowic_compress_start_time = datetime.now()
        self.pre_train_model()

        log_base_name = os.path.basename(self.logpath).split('.')[0]
        compress_cmd = 'bin/compressor_cmd_tool -c {log_file} -m {model_path} -o {output_path}'.format(
            log_file=self.logpath,
            model_path=self.model_path,
            output_path=os.path.join(self.out_dir, log_base_name)
        )

        subprocess.Popen(compress_cmd, shell=True, cwd='../cowic').wait()
        if not os.path.isfile(os.path.join(self.out_dir, log_base_name) + '.dat'):
            raise FileNotFoundError('Fail to compress')
        cowic_compress_time = datetime.now()-cowic_compress_start_time
        cowic_compress_time = cowic_compress_time.total_seconds()

        cowic_mdls_size = sum([os.path.getsize(os.path.join(self.out_dir, x)) for x in os.listdir(self.out_dir)])

        return {
            'CowicCompressTime': cowic_compress_time,
            'CowicCompressSize': cowic_mdls_size
        }
