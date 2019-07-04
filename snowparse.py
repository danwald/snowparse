#!/usr/bin/env python
from __future__ import print_function
import sys
import base64
import io
import json
import re
import binascii
from collections import Counter

def add_padding(data):
    data= data.strip()
    return (data + '='*(len(data)%4)) if (len(data) % 4) else data

def get_start_index(string, sub_str='cx":"', fr=0):
    return string.find(sub_str, fr)

def get_schema_match(string):
    match = re.search(r'iglu:com.dubizzle\\*/([-A-Za-z0-9_]*)\\*/jsonschema', string)
    return match.group(1) if match else None
def extract_data_str(string, grouped_regex_str=r'&cx=([-A-Za-z0-9+=]*)', group=1):
    match = re.search(grouped_regex_str, string)
    return match.group(group) if match else None

def get_decoded_str(b64_str):
    try:
        padded_str = add_padding(b64_str)
        b_str=base64.b64decode(padded_str)
        return str(b_str, 'utf-8', 'ignore')
    except Exception as e:
        print(e,len(b64_str), len(padded_str))
        return ''
def is_moderated(pstring):
    return re.search(r'(deletedByUserId|hermes)', pstring, flags=re.IGNORECASE)

def parse():
    counts=Counter()
    schemas=Counter()
    current=0
    for line in io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8'):
        current+=1
        try:
            coded_string=json.loads(line)['line']
            #print("CODED STRING\n", coded_string)
            byte_str=base64.b64decode(coded_string)
            #print("B64DeCODEd\n", byte_str)
            py_str=str(byte_str, 'utf-8', 'ignore')
            #print("PY_STR\n", py_str)
            start_index=get_start_index(py_str)
            if start_index != -1:
                start_index += 5
                end_index=get_start_index(py_str, sub_str='"', fr=start_index)
                #print("sTART:", start_index,"End:", end_index)
                sub_str=py_str[start_index:end_index]
                #print("sub_str\n", sub_str)
                byte_sub_str=base64.b64decode(add_padding(sub_str))
                py_sub_str=str(byte_sub_str, 'utf-8', 'ignore')
                schemas.update({json.loads(py_sub_str)['data'][0]['schema']:1})
                counts.update({'good':1})
            else:
                #print("EXTRACTING DATA")
                match = re.search(r'cx=([-A-Za-z0-9+=].*)\^', py_str)
                if match:
                    sub_str = match.group(1)
                    #print("sub_str\n", sub_str)
                    byte_sub_str=base64.b64decode(add_padding(sub_str))
                    #print("B64DeCODEd\n", byte_sub_str)
                    py_sub_str=str(byte_sub_str, 'utf-8', 'ignore')
                    #print("PY_STR\n", py_sub_str)
                    match = get_schema_match(py_sub_str)
                    if match:
                        schemas.update({match:1})
                        counts.update({'good':1})
                    else:
                        counts.update({'bad':1})
                        #print("No schema found")
                        sys.stderr.write(line)
                else:
                    match = get_schema_match(py_str)
                    if match:
                        schemas.update({match:1})
                        counts.update({'good':1})
                    else:
                        match = extract_data_str(py_str,
                        grouped_regex_str=r'"ue_px":"([-A-Za-z0-9+=]*)"')
                        if match:
                            py_str = get_decoded_str(match)
                            if py_str:
                                match = get_schema_match(py_str)
                                if match:
                                    schemas.update({match:1})
                                    counts.update({'good':1})
                                else:
                                    if not is_moderated(line):
                                        counts.update({'badS4':1})
                                        sys.stderr.write(line)
                                        print("No 4th schema found")
                                    else:
                                        schemas.update({'moderated_ad':1})
                                        counts.update({'good':1})
                            else:
                                if not is_moderated(line):
                                    counts.update({'badb4':1})
                                    sys.stderr.write(line)
                                    print("No 4th b64decode found")
                                else:
                                    schemas.update({'moderated_ad':1})
                                    counts.update({'good':1})

                        else:
                            counts.update({'bad':1})
                            print("No 3rd schema found")
                            sys.stderr.write(py_str)

        except KeyError as k:
            counts.update({'key':1})
            sys.stdout.write('\n{} Error[{}] parsing line {}'.format(type(k), k, current))
            sys.stderr.write(line)
        except binascii.Error as b:
            match = extract_data_str(py_str)
            if match:
                try:
                    byte_sub_str=base64.b64decode(add_padding(match))
                except binascii.Error as b2:
                    counts.update({'binasccii2':1})
                    sys.stdout.write('\n{} Error[{}] parsing line {}'.format(type(b), b, current))
                else:
                    py_sub_str=str(byte_sub_str, 'utf-8', 'ignore')
                    match = get_schema_match(py_sub_str)
                    if match:
                        schemas.update({match:1})
                        counts.update({'good':1})
                    else:
                        counts.update({'bad2':1})
                        sys.stderr.write(line)
            else:
                counts.update({'binasccii':1})
                sys.stdout.write('\n{} Error[{}] parsing line {}'.format(type(b), b, current))
                sys.stderr.write(line)
        except Exception as e:
            counts.update({e:1})
            sys.stdout.write('\n{} Error[{}] parsing line {}'.format(type(e), e,current))
            sys.stderr.write(line)
        else:
            sys.stdout.write('\rProcessing {} lines'.format(sum(counts.values())))

    print('\nProcessed {} lines.({} failure(s))'.format(
        sum(counts.values()),
        sum({v for k,v in counts.items() if k != 'good'})
        )
    )
    print(schemas)
    print(counts)
    return 0

if __name__ == "__main__":
    sys.exit(parse())
