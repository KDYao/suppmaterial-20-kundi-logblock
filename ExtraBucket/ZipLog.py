"""
Code reference to Logpai/Logzip repository
"""

import os
import tarfile
import glob
import re
import json
import sys
import time
import gc
from itertools import zip_longest
import subprocess
import argparse


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


class Ziplog:
    def __init__(self, outdir, outname, kernel="gz", tmp_dir="", level=3, lossy=False, n_workers=1,
                 compress_single=True, isCompress=False, isKeepTemplate=False):
        self.outdir = outdir
        self.outname = outname
        self.kernel = kernel
        self.io_time = 0
        self.level = level
        self.lossy = lossy
        self.tmp_dir = tmp_dir
        self.n_workers = n_workers
        self.compress_single = compress_single
        self.isCompress = isCompress
        self.isKeepTemplate = isKeepTemplate
        self.splitting_time = 0
        self.packing_time = 0

    def compress_all(self):
        self.file_all_column_dict = {}
        ignore_columns = ["LineId", "EventTemplate", "ParameterList", "EventId"]
        focus_columns = [col for col in self.para_df.columns if col not in ignore_columns]
        for column in focus_columns:
            filename = column + "_0"
            self.file_all_column_dict[filename] = self.para_df[column]
        del self.para_df

    def compress_normal(self):
        ignore_columns = ["LineId", "EventTemplate", "ParameterList", "EventId", "Content"]
        focus_columns = [col for col in self.para_df.columns if col not in ignore_columns]

        splited_columns = split_normal(self.para_df[focus_columns])

        self.para_df.drop(focus_columns, axis=1, inplace=True)

        self.file_normal_column_dict = {}
        for idx, colname in enumerate(focus_columns):
            columns_t = list(zip_longest(*splited_columns[idx], fillvalue=""))  # transpose
            for sub_idx, col in enumerate(columns_t):
                filename = f"{colname}_{sub_idx}"
                self.file_normal_column_dict[filename] = col
        self.file_normal_column_dict["EventId_0"] = self.para_df["EventId"]

    def __pack_params(self, dataframe):
        '''
        Input: dataframe with tow columns [EventId, ParameterList]
        '''
        self.file_para_dict = {}
        for eid in dataframe["EventId"].unique():
            paras = dataframe.loc[dataframe["EventId"] == eid, "ParameterList"]
            paracolumns = list(zip_longest(*paras, fillvalue=""))
            for para_idx, subparas in enumerate(paracolumns):
                subparas_columns = list(zip_longest(*subparas, fillvalue=""))
                for sub_para_idx, sub_subparas in enumerate(subparas_columns):
                    filename = f"{eid}_{para_idx}_{sub_para_idx}"
                    self.file_para_dict[filename] = sub_subparas

    def __build_para_index(self):
        index = 0
        para_index_dict = {}
        for filename, paras in self.file_para_dict.items():
            para_set = set(paras)
            for upara in para_set:
                index += 1
                index_64 = baseN(index, 64)
                para_index_dict[upara] = index_64
            paras_mapped = [para_index_dict[para] for para in paras]
            self.file_para_dict[filename] = paras_mapped
        self.index_para_dict = {v: k for k, v in para_index_dict.items()}

    def compress_content(self):
        self.template_mapping = dict(zip(self.para_df["EventId"], \
                                         self.para_df["EventTemplate"]))

        eids = [eid for eid in self.template_mapping \
                if "<*>" in self.template_mapping[eid]]
        print("{} events to be split.".format(len(eids)))

        #        eids = eids[0:1]

        focus_index = self.para_df["EventId"].isin(eids)
        focus_df = self.para_df.loc[focus_index, \
                                    ["EventId", "ParameterList"]]
        del self.para_df

        splitted_para = split_para(focus_df["ParameterList"])

        focus_df["ParameterList"] = splitted_para

        self.__pack_params(focus_df)

        if self.level == 3:
            self.__build_para_index()

        gc.collect()

    def __kernel_compress(self):
        '''
        level1 : only normal
        [self.file_all_column_dict]
        ---
        level2 : parse without index
        [self.file_para_dict, self.file_normal_column_dict]
        ---
        leve3: parse and index
        [self.file_para_dict, self.file_normal_column_dict]
        '''

        def output_dict(adict):
            for filename, content_list in adict.items():
                with open(os.path.join(self.tmp_dir, filename + ".csv"), "w") as fw:
                    fw.writelines("\n".join(list(content_list)))

        def files_to_tar(filepaths):
            worker_id = os.getpid()
            print("Worker {} start taring {} files.".format(worker_id, len(filepaths)))
            for idx, filepath in enumerate(filepaths, 1):
                if len(filepaths) > 10 and idx % (len(filepaths) // 10) == 0:
                    print("Worker {}, {}/{}".format(worker_id, idx, len(filepaths)))
                if self.kernel == "gz" or self.kernel == "bz2":
                    tar = tarfile.open(filepath + ".tar.{}".format(self.kernel), \
                                       "w:{}".format(self.kernel))
                    tar.add(filepath, arcname=os.path.basename(filepath))
                    tar.close()
                elif self.kernel == "lzma":
                    os.system('lzma -k {}'.format(filepath))

                    ## output begin

        if self.level == 3 and not self.lossy:
            with open(os.path.join(self.tmp_dir, "parameter_mapping.json"), "w") as fw:
                json.dump(self.index_para_dict, fw)
        if self.isKeepTemplate:
            if self.level > 1:
                with open(os.path.join(self.tmp_dir, "template_mapping.json"), "w") as fw:
                    json.dump(self.template_mapping, fw)

        if self.level == 1:
            output_dict(self.file_all_column_dict)
        elif self.level == 2 or self.level == 3:
            if not self.lossy:
                output_dict(self.file_para_dict)
            output_dict(self.file_normal_column_dict)
        else:
            raise RuntimeError(f"The level {self.level} is illegal!")
        ## output end

        #        self.compress_single
        ## compress begin
        if self.isCompress:

            if self.kernel in set(["gz", "bz2"]):
                raw_files = glob.glob(os.path.join(self.tmp_dir, "*.csv")) \
                            + glob.glob(os.path.join(self.tmp_dir, "*.json"))
                tarall = tarfile.open(os.path.join(self.outdir, \
                                                   "{}.tar.{}".format(self.outname, self.kernel)), \
                                      "w:{}".format(self.kernel))
                if self.compress_single:
                    files_to_tar(raw_files)
                    files = glob.glob(os.path.join(self.tmp_dir, \
                                                   "*.tar.{}".format(self.kernel)))
                else:
                    files = raw_files

                for idx, filepath in enumerate(files, 1):
                    tarall.add(filepath, arcname=os.path.basename(filepath))
                tarall.close()
        ## compress end

    def zip_file(self, para_df=None, delete_tmp=True):
        self.para_df = para_df.fillna("")

        t1 = time.time()
        if self.level == 1:
            self.compress_all()
        elif self.level == 2 or self.level == 3:
            self.compress_normal()
            self.compress_content()

        t2 = time.time()
        self.__kernel_compress()

        t3 = time.time()

        self.splitting_time = t2 - t1
        self.packing_time = t3 - t2
