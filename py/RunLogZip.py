"""
Code refers to logzip_demo.py from logzip project
"""
import sys
import os
import subprocess
import time
import re
import argparse
sys.path.insert(1, os.path.realpath(os.path.pardir))
from logzip_public.logzip_demo import Ziplog
from logzip_public.matcher import treematch
from logzip_public.logparser import Drain


def boolean_string(s):
    if s not in {'False', 'True'}:
        raise ValueError('Not a valid boolean string')
    return s == 'True'

split_regex = re.compile("([^a-zA-Z0-9]+)")

def split_item(astr):
    return split_regex.split(astr)

def split_list(alist):
#    print(alist)
    return list(map(split_item, alist))

def split_para(seires):
    return seires.map(split_list)

def split_normal(dataframe):
    return [dataframe[col].map(split_item).tolist() \
            for col in dataframe.columns]

def baseN(num, b):
    if isinstance(num, str):
        num = int(num)
    if num is None: return ""
    return ((num == 0) and "0") or \
            (baseN(num // b, b).lstrip("0") + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+="[num % b])


class RunZipLog:
    def __init__(self, dataset, setting, path, isCompress, out_dir="./zip_out/", tmp_dir="./zip_out/tmp_dir"):
        self.args = None
        self.st = setting['st']
        self.depth = setting['depth']
        self.regex = setting['regex']
        self.isCompress=isCompress
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument('--file', type=str, default=path)
            parser.add_argument('--log_format', type=str, default=setting['log_format'])
            parser.add_argument('--template_file', type=str, default="")
            #"../data/bucket_%s" % os.path.basename(path).split(os.extsep)[0]
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

        """
        0. sampling
        """

        line_num = subprocess.check_output("wc -l {}".format(filepath), shell=True)
        line_num = int(line_num.split()[0])
        sample_file_path = filepath + ".sample"
        try:
            subprocess.check_output("gshuf -n{} {} > {}".format(sample_num, filepath,
                                                                sample_file_path), shell=True)
        except Exception:
            subprocess.check_output("shuf -n{} {} > {}".format(sample_num, filepath,
                                                               sample_file_path), shell=True)

        """
        1. get template file  
        """
        st = self.st  # Similarity threshold
        depth = self.depth  # Depth of all leaf nodes
        regex = self.regex

        parse_begin_time = time.time()
        parser = Drain.LogParser(log_format, outdir=out_dir, depth=depth, st=st, rex=regex)
        templates = parser.parse(sample_file_path)
        os.remove(sample_file_path)
        parse_end_time = time.time()
        print("Parser cost [{:.3f}s]".format(parse_end_time - parse_begin_time))

        matcher_begin_time = time.time()
        if template_file:
            with open(template_file) as fr:
                templates = [item.strip() for item in fr.readlines()]
        matcher = treematch.PatternMatch(tmp_dir=tmp_dir, outdir=out_dir, logformat=log_format)
        structured_log = matcher.match(filepath, templates)
        matcher_end_time = time.time()
        print("Matcher cost [{:.3f}s]".format(matcher_end_time - matcher_begin_time))

        zipper_begin_time = time.time()
        zipper = Ziplog(outdir=out_dir,
                        outname=outname,
                        kernel=kernel,
                        tmp_dir=tmp_dir,
                        level=level,
                        lossy=lossy,
                        compress_single=compress_single,
                        n_workers=n_workers,
                        isCompress=self.isCompress)

        zipper.zip_file(para_df=structured_log)
        zipper_end_time = time.time()
        print("Zipper cost [{:.3f}s]".format(zipper_end_time - zipper_begin_time))

        self.stuctured_log = structured_log
        return zipper_end_time-zipper_begin_time