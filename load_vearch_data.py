# -*- coding: utf-8 -*-
"""
python read_vearch_data.py -f ./ -t 1 --int64 -s ./data/docs.txt
"""
import sys
import time
import os
import requests
import json
import struct
import random
import argparse
import rocksdb
from multiprocessing import Process, Queue
from concurrent.futures import ThreadPoolExecutor, as_completed


class Vector(object):
    """
    当编译gamma的时候添加了 `-DTABLE_STR_INT64=ON` 参数, int64_flag设置成True
    """
    data_type_list = ['int', 'long', 'float', 'double', 'string', 'vector']
    def __init__(self, filepath, table_name, int64_flag=False):
        """
        filepath: gamma的data目录
        table_name: data目录下文件夹名字，一般是以数字命名，代表表名
        int64_flag： 默认为False，当编译gamma的时候添加了 `-DTABLE_STR_INT64=ON` 参数时, 设置为True
        """
        self.filepath = filepath
        self.table_name = table_name
        self.field_names = []
        self.field_types = []
        self.vector_names = []
        self.vector_types = []
        self.dimensions = []
        self.bitmap = {}
        self.ids = {}
        self.int64_flag = int64_flag
        self.docs = []

    def read_schema(self):
        schema_file = os.path.join(self.filepath, self.table_name, self.table_name+'.schema')
        with open(schema_file, 'rb') as f:
            index_num = struct.unpack('i', f.read(4))[0]
            field_num = struct.unpack('i', f.read(4))[0]
            for _ in range(field_num):
                field_name = f.read(struct.unpack('i', f.read(4))[0]).decode()
                self.field_names.append(field_name)
                field_type = struct.unpack('H', f.read(2))[0]
                self.field_types.append(field_type)
                f.seek(1, 1)

            # read vector
            vector_num = struct.unpack('i', f.read(4))[0]
            for _ in range(vector_num):
                vector_name = f.read(struct.unpack('i', f.read(4))[0]).decode()
                self.vector_names.append(vector_name)
                field_type = struct.unpack('H', f.read(2))[0]
                self.vector_types.append(field_type)
                # id_index
                f.seek(1, 1)
                dimension = struct.unpack('i', f.read(4))[0]
                self.dimensions.append(dimension)

            print(f'schema info: [index_num: {index_num}, field_num: {field_num}, field_names: {self.field_names}, field_types: {[self.data_type_list[i] for i in self.field_types]}, vector_num: {vector_num}, vector_names: {self.vector_names}, dimensions: {self.dimensions}]')

    def read_profile(self):
        profile_dir = os.path.join(self.filepath, self.table_name, 'table')
        index = 0
        count = 0
        while True:
            profile_path = os.path.join(profile_dir, f'{index}.profile')
            # 字符串字段单独存储， 如果没有字符串字段则不存在该文件
            str_path = os.path.join(profile_dir, f'{index}.str.profile')
            if not os.path.exists(profile_path):
                break

            index = index + 1
            with open(profile_path, 'rb') as f, open(str_path, 'rb') as fs:
                f.seek(5, 0)
                size = struct.unpack('i', f.read(4))[0]
                if self.int64_flag:
                    f.seek(54, 0)
                else:
                    f.seek(46, 0)
                print(f'profile_path: {profile_path}, size: {size}')
                for n in range(size):
                    doc = {}
                    for i, field_type in enumerate(self.field_types):
                        if field_type == 0:
                            doc[self.field_names[i]] = struct.unpack('i', f.read(4))[0]
                        elif field_type == 1:
                            doc[self.field_names[i]] = struct.unpack('Q', f.read(8))[0]
                        elif field_type == 2:
                            doc[self.field_names[i]] = struct.unpack('f', f.read(4))[0]
                        elif field_type == 3:
                            doc[self.field_names[i]] = struct.unpack('d', f.read(8))[0]
                        elif field_type == 4:
                            if self.int64_flag:
                                fs.seek(struct.unpack('l', f.read(8))[0], 0)
                                value = fs.read(struct.unpack('H', f.read(2))[0]).decode()
                            else:
                                fs.seek(struct.unpack('I', f.read(4))[0], 0)
                                value = fs.read(struct.unpack('B', f.read(1))[0]).decode()
                            doc[self.field_names[i]] = value
                        else:
                            print(f'field type[{field_type}] is not found')
                            os._exit(-1)
                    self.docs.append(doc)
                    # 记录重复的id，如果该id已经存在则表示之前的是已经被删除
                    if doc['_id'] in self.ids:
                        self.bitmap[self.ids[doc['_id']]] = True
                    self.ids[doc['_id']] = count
                    count = count + 1
                    #  if n > 50000: break
            #  break
        print(f'load profile finished, docs num: {count}, delete num: {len(self.bitmap)}')

    def read_bitmap(self):
        pass

    def read_vectors(self):
        for i, vector_name in enumerate(self.vector_names):
            vector_file = os.path.join(self.filepath, self.table_name, 'vectors', f'{vector_name}.000')
            db = rocksdb.DB(vector_file, rocksdb.Options(create_if_missing=True))
            it = db.iterkeys()
            it.seek_to_first()
            count = 0
            for key in it:
                feature = list(struct.unpack(f'{self.dimensions[i]}f', db.get(key)))
                self.docs[count][vector_name] = {'feature': feature}
                count = count + 1
                #  if vec_index > 50000: break
            self.docs = self.docs[: count]
            print(f'load vector finished, vec num: {count}')

    def insert(self, url):
        for index, doc in enumerate(self.docs):
            if self.vector_names[0] not in doc:
                break
            if index in self.bitmap:
                continue
            response = requests.post(url + doc.pop('_id'), data=json.dumps(doc))
            if response.status_code != 200:
                print(f'response failed, [{response.text}]')

    def insert_multi(self, url):
        request_process = 1
        dump_process = 10
        data_queue = Queue(2000)
        res_queue = Queue(100)
        def deal():
            res_queue.put(1)
            pool = ThreadPoolExecutor(200)
            session = requests.sessions.Session()
            futures = []
            end_num = 0
            while True:
                body = data_queue.get(timeout=100)
                #  print(data_queue.qsize())
                if body is None:
                    end_num =end_num + 1
                    if end_num >= dump_process: break
                    continue
                futures.append(pool.submit(session.request, "post", url+'_bulk', data=body))
            for f in as_completed(futures):
                if f.exception():
                    print(f.exception())
                else:
                    pass
            res_queue.get()
            print('all dump process has finished!', end_num)

        process_list = []
        for i in range(request_process):
            process_list.append(Process(target=deal, name="consumer"))
        for p in process_list:
            p.daemon = True
            p.start()

        doc_queue = Queue(2000)
        def dump():
            count = 0
            s = ''
            while True:
                doc = doc_queue.get(timeout=100)
                if doc is None:
                    break
                count = count + 1
                d = {"index": {"_id": doc.pop('_id')}}
                s = s + json.dumps(d) + "\n" + json.dumps(doc) + "\n"
                if count != 0 and count % 50 == 0:
                    data_queue.put(s)
                    s = ''
            if s != '': data_queue.put(s)
            data_queue.put(None)
            print(f'process {os.getpid()} dump num: {count}')
                
        process_list = []
        for i in range(dump_process):
            process_list.append(Process(target=dump, name=f'dump-process-{i}'))
        for p in process_list:
            p.daemon = True
            p.start()


        for idx, doc in enumerate(self.docs):
            if self.vector_names[0] not in doc:
                break
            if idx in self.bitmap:
                continue
            doc_queue.put(doc)
        for i in range(dump_process): doc_queue.put(None)
        while not res_queue.empty():
            time.sleep(1)
        print('all data has finished!')

    def save(self, savepath):
        with open(savepath, 'w') as fw:
            for index, doc in enumerate(self.docs):
                if self.vector_names[0] not in doc:
                    break
                if index in self.bitmap:
                    continue
                fw.write(json.dumps(doc) + '\n')


def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--filepath', type=str, default='./',
                        help='filepath of gamma data')
    parser.add_argument('-t', '--table_name', type=str, default='1',
                        help='name of table')
    parser.add_argument('-u', '--url', type=str, default=None,
                        help='router url')
    parser.add_argument('-s', '--savepath', type=str, default=None,
                        help='local path')
    parser.add_argument('--int64', action='store_true',
                        help='whether DTABLE_STR_INT64')
    args = parser.parse_args()
    print(args)
    return args


if __name__ == '__main__':
    args = parse_args()
    filepath = args.filepath
    if not os.path.exists(filepath):
        print(f'filepath[{filepath}] does not exist!')
        os._exit(0)
    if not os.path.exists(os.path.join(filepath, args.table_name)):
        print(f'table[{args.table_name}] does not exist!')
        os._exit(0)
    if args.savepath and not os.path.exists(os.path.dirname(args.savepath)):
        print(f'table[{args.savepath}] does not exist!')
        os._exit(0)
    vector = Vector(filepath, args.table_name, args.int64)
    vector.read_schema()
    vector.read_profile()
    vector.read_vectors()

    if args.url:
        vector.insert_multi(args.url)
    elif args.savepath:
        vector.save(args.savepath)
