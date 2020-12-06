"""
Introducing another Preprocessing approach by extracting tempaltes from large logs and apply it to splitted log blocks
built by introducing a bucketing step in logzip
"""
import argparse
import os
import sys
import time
import re
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
    def __init__(self, df_template, setting, path, out_dir, tmp_dir, isCompress, match_tree=None):
        self.args = None
        self.df_template = df_template
        self.st = setting['st']
        self.depth = setting['depth']
        self.regex = setting['regex']
        self.isCompress = isCompress

        self.filepath = path
        self.kernel = 'gz'
        self.log_format = setting['log_format']
        self.compress_single = False
        self.n_workers = 1
        self.level = 3
        self.tmp_dir = tmp_dir
        self.out_dir = out_dir
        self.lossy = False
        self.match_tree = match_tree



    def run(self):
        filepath = self.filepath
        kernel = self.kernel
        log_format = self.log_format
        compress_single = self.compress_single
        n_workers = self.n_workers
        level = self.level
        tmp_dir = self.tmp_dir
        out_dir = self.out_dir
        lossy = self.lossy

        logname = os.path.basename(filepath)
        outname = logname + ".logzip"
        print("Tmp files are in {}".format(tmp_dir))

        if not os.path.isdir(tmp_dir):
            os.makedirs(tmp_dir)
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)

        matcher_begin_time = datetime.now()
        matcher = treematch.PatternMatch(tmp_dir=tmp_dir, outdir=out_dir, logformat=log_format)
        structured_log = matcher.match(filepath, self.df_template['EventTemplate'], match_tree=self.match_tree)
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
        self.match_time = (matcher_end_time - matcher_begin_time).total_seconds()
        return zipper_end_time - zipper_begin_time


class BuildTree:
    def message_split(self, message):
        """
        Code from logzip
        :param message:
        :return:
        """
        splitter_regex = re.compile('(<\*>|[^A-Za-z])')
        tokens = re.split(splitter_regex, message)
        tokens = list(filter(lambda x: x!='', tokens))
        tokens = [token for idx, token in enumerate(tokens) if token != '' and not (token == "<*>" and idx > 0 and tokens[idx-1]=="<*>")]
        # print(tokens)
        return tokens

    def _preprocess_template(self, template):
        """
        Code from logzip
        :param message:
        :return:
        """
        template = re.sub("<NUM>", "<*>", template)
        if template.count("<*>") > 5:
            first_start_pos = template.index("<*>")
            template = template[0:first_start_pos + 3]
        return template

    def build_match_tree(self, templates):
        """
        Code from Logzip
        :param templates:
        :return:
        """
        match_tree = {}
        match_tree["$NO_STAR$"] = {}
        for event_id, event_template in templates:
            print(event_id)
            # Full match
            if "<*>" not in event_template:
                match_tree["$NO_STAR$"][event_template] = event_template
                continue
            event_template = self._preprocess_template(event_template)
            template_tokens = self.message_split(event_template)
            if not template_tokens or event_template=="<*>": continue

            start_token = template_tokens[0]
            if start_token not in match_tree:
                match_tree[start_token] = {}
            move_tree = match_tree[start_token]

            tidx = 1
            while tidx < len(template_tokens):
                token = template_tokens[tidx]
                if token not in move_tree:
                    move_tree[token] = {}
                move_tree = move_tree[token]
                tidx += 1
            move_tree["".join(template_tokens)] = (len(template_tokens), template_tokens.count("<*>")) # length, count of <*>
        return match_tree

    def read_templates(self, templates):
        """
        Code from Logzip
        :param templates:
        :return:
        """
        templates_save = []
        for idx, row in enumerate(templates):
            event_id = "E" + str(idx)
            event_template = row
            templates_save.append((event_id, event_template))
        return templates_save