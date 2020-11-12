"""
Introducing another Preprocessing approach by extracting tempaltes from large logs and apply it to splitted log blocks
built by introducing a bucketing step in logzip
"""
import argparse
import os
import sys
import time
from datetime import datetime
from ExtraBucket import Drain, treematch, ZipLog

benchmark_settings_local = {
    'HDFS': {
        'log_file': 'HDFS/HDFS_10M.log',
        'log_format': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
        'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
        'st': 0.5,
        'depth': 4
        },

    'Hadoop': {
        'log_file': 'Hadoop/Hadoop_10M.log',
        'log_format': '<Date> <Time> <Level> \[<Process>\] <Component>: <Content>',
        'regex': [r'(\d+\.){3}\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Spark': {
        'log_file': 'Spark/Spark_10M.log',
        'log_format': '<Date> <Time> <Level> <Component>: <Content>',
        'regex': [r'(\d+\.){3}\d+', r'\b[KGTM]?B\b', r'([\w-]+\.){2,}[\w-]+'],
        'st': 0.5,
        'depth': 4
        },

    'Zookeeper': {
        'log_file': 'Zookeeper/Zookeeper_10M.log',
        'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
        'regex': [r'(/|)(\d+\.){3}\d+(:\d+)?'],
        'st': 0.5,
        'depth': 4
        },

    'BGL': {
        'log_file': 'BGL/BGL_10M.log',
        'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
        'regex': [r'core\.\d+'],
        'st': 0.5,
        'depth': 4
        },

    'HPC': {
        'log_file': 'HPC/HPC_10M.log',
        'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
        'regex': [r'=\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Thunderbird': {
        'log_file': 'Thunderbird/Thunderbird_10M.log',
        'log_format': '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
        'regex': [r'(\d+\.){3}\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Windows': {
        'log_file': 'Windows/Windows_10M.log',
        'log_format': '<Date> <Time>, <Level>                  <Component>    <Content>',
        'regex': [r'0x.*?\s'],
        'st': 0.7,
        'depth': 5
        },

    'Linux': {
        'log_file': 'Linux/Linux_10M.log',
        'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
        'regex': [r'(\d+\.){3}\d+', r'\d{2}:\d{2}:\d{2}'],
        'st': 0.39,
        'depth': 6
        },

    'Android': {
        'log_file': 'Android/Android_10M.log',
        'log_format': '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
        'regex': [r'(/[\w-]+)+', r'([\w-]+\.){2,}[\w-]+', r'\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b'],
        'st': 0.2,
        'depth': 6
        },

    'HealthApp': {
        'log_file': 'HealthApp/HealthApp_10M.log',
        'log_format': '<Time>\|<Component>\|<Pid>\|<Content>',
        'regex': [],
        'st': 0.2,
        'depth': 4
        },

    'Apache': {
        'log_file': 'Apache/Apache_10M.log',
        'log_format': '\[<Time>\] \[<Level>\] <Content>',
        'regex': [r'(\d+\.){3}\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Proxifier': {
        'log_file': 'Proxifier/Proxifier_10M.log',
        'log_format': '\[<Time>\] <Program> - <Content>',
        'regex': [r'<\d+\ssec', r'([\w-]+\.)+[\w-]+(:\d+)?', r'\d{2}:\d{2}(:\d{2})*', r'[KGTM]B'],
        'st': 0.6,
        'depth': 3
        },

    'OpenSSH': {
        'log_file': 'OpenSSH/OpenSSH_10M.log',
        'log_format': '<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>',
        'regex': [r'(\d+\.){3}\d+', r'([\w-]+\.){2,}[\w-]+'],
        'st': 0.6,
        'depth': 5
        },

    'OpenStack': {
        'log_file': 'OpenStack/OpenStack_10M.log',
        'log_format': '<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>',
        'regex': [r'((\d+\.){3}\d+,?)+', r'/.+?\s', r'\d+'],
        'st': 0.5,
        'depth': 5
        },

    'Mac': {
        'log_file': 'Mac/Mac_10M.log',
        'log_format': '<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>',
        'regex': [r'([\w-]+\.){2,}[\w-]+'],
        'st': 0.7,
        'depth': 6
        },
}


class ParseTemplate:
    """
    Parse templates from the whole log
    """
    def __init__(self, logName, setting, out_dir=''):
        self.logName = logName
        self.logFormat = setting['log_format']
        self.regex = setting['regex']
        self.st = setting['st']
        self.depth = setting['depth']

    def parse_template_from_all(self, fp):

        parse_begin_time = datetime.now()
        parser = Drain.LogParser(log_name=self.logName, log_format=self.logFormat, depth=self.depth, st=self.st, rex=self.regex)
        return parser.parse(fp)

def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'


class RunExtraBucket:
    """
    The code refer to logpai's parser
    """
    def __init__(self, df_template, setting, path, out_dir, tmp_dir, isCompress):
        self.args = None
        self.df_template = df_template
        self.st = setting['st']
        self.depth = setting['depth']
        self.regex = setting['regex']
        self.isCompress = isCompress

        try:
            parser = argparse.ArgumentParser()
            parser.add_argument('--file', type=str, default=path)
            parser.add_argument('--log_format', type=str, default=setting['log_format'])
            parser.add_argument('--template_file', type=str, default="")
            parser.add_argument('--tmp_dir', type=str, default=tmp_dir)
            parser.add_argument('--out_dir', type=str, default=out_dir)
            parser.add_argument('--compress_single', type=boolean_string, default=False)
            parser.add_argument('--n_workers', type=int, default=3)
            parser.add_argument('--level', type=int, default=3)
            parser.add_argument('--kernel', type=str, default="gz")
            parser.add_argument('--sample_num', type=int, default=10000)
            parser.add_argument('--lossy', type=boolean_string, default=False)
            self.args = vars(parser.parse_args())
        except Exception as e:
            print(e)
            pass

    def run(self):
        filepath = self.args["file"]
        kernel = self.args["kernel"]
        log_format = self.args["log_format"]
        template_file = self.args["template_file"]
        compress_single = self.args["compress_single"]
        sample_num = self.args["sample_num"]
        n_workers = self.args["n_workers"]
        level = self.args["level"]
        tmp_dir = self.args["tmp_dir"]
        out_dir = self.args["out_dir"]
        lossy = self.args["lossy"]

        logname = os.path.basename(filepath)
        outname = logname + ".logzip"
        print("Tmp files are in {}".format(tmp_dir))

        if not os.path.isdir(tmp_dir):
            os.makedirs(tmp_dir)
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)

        matcher_begin_time = datetime.now()
        matcher = treematch.PatternMatch(tmp_dir=tmp_dir, outdir=out_dir, logformat=log_format)
        structured_log = matcher.match(filepath, self.df_template['EventTemplate'])
        matcher_end_time = datetime.now()
        #print("Matcher cost [{:.3f}s]".format(matcher_end_time - matcher_begin_time))
        print("Matcher cost {}".format(str(matcher_end_time - matcher_begin_time)))

        zipper_begin_time = datetime.now()

        zipper = ZipLog.Ziplog(outdir=out_dir,
                        outname=outname,
                        kernel=kernel,
                        tmp_dir=tmp_dir,
                        level=level,
                        lossy=lossy,
                        compress_single=compress_single,
                        n_workers=n_workers,
                        isCompress=self.isCompress)

        zipper.zip_file(para_df=structured_log)
        zipper_end_time = datetime.now()
        #print("Zipper cost [{:.3f}s]".format(zipper_end_time - zipper_begin_time))
        print("Zipper cost {}".format(str(zipper_end_time - zipper_begin_time)))

        self.stuctured_log = structured_log
        return zipper_end_time - zipper_begin_time


