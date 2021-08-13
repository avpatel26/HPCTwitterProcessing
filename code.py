import json
import collections
import operator
from mpi4py import MPI
import numpy as np
import re
from datetime import datetime

start_time = datetime.now()
comm = MPI.COMM_WORLD
size = comm.Get_size()
my_rank = comm.Get_rank()

file = open("tinyTwitter.json", mode='r', encoding='utf-8')
lang_count ={}
lang_dict = {}
dict_of_tweets = {}
tweets=[]


# count and add unique hashtag
def tweetsToDict(tweetlist, tweetDict):
    for i in tweetlist:
        tweet = i.encode("UTF-8")
        if tweet in tweetDict:
            tweetDict[tweet] += 1
        else:
            tweetDict[tweet] = 1
    return tweetDict


# count and add unique language
def count_language(language):
    if language in lang_dict:
        language = lang_dict.get(language) + "(" + language + ")"
    if language in lang_count:
        lang_count[language] += 1
    else:
        lang_count[language] = 1


# load language code
with open("Language_code") as f:
    for line in f:
        value,part,key = line.strip().partition(" ")
        lang_dict[key] = value


# Extract necessary data from json
def load_data(line):
    data = {}
    if "lang" in line:
        json_line = json.loads(line.rstrip(",\n"))
        data["lang"] = json_line['doc']['lang']
    if "text" in line:
        data["text"] = re.findall('(?:\#+[\w_]+[\w\'_\-]*[\w_]+)', line)
    return data


# Sort and get top 10 values
def sort_data(dictionary):
    return sorted(dictionary.items(), key=operator.itemgetter(1), reverse=True)[:10]


# print final result
def print_result(dict):
    for data in dict :
        print(data)


# combining dictionaries after gathering from different process
def merge_dictionaries(list):
    counter = collections.Counter()
    for data in list:
        try:
            counter.update(data)
        except:
            continue
    return dict(counter)


if size< 2 and my_rank == 0 :   #for 1 node 1 core
    final_data =[]
    for line in file:
        try:
            data = {}
            data = load_data(line)
            final_data.append(data)     # Append data to final list
        except:
            continue
    for data in final_data:
        try:
            count_language(data["lang"])
            tweetsToDict(data["text"],dict_of_tweets)
        except:
             continue
    lang_count = sort_data(lang_count)
    dict_of_tweets = sort_data(dict_of_tweets)
    print("Top 10 HashTag:")
    print_result(dict_of_tweets)
    print("\nTop 10 Languages:")
    print_result(lang_count)
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
    exit()
elif my_rank==0 :
    final_data=[]
    for line in file:
        try:
            data = {}
            data = load_data(line)
            final_data.append(data)
        except:
            continue
    part_data = np.array_split(final_data,size)         # Split array by master process 0
else:
    part_data = None

part_data = comm.scatter(part_data, root=0)             # Scatter data to different process
for data in part_data:
    try:
        count_language(data["lang"])
        tweetsToDict(data["text"], dict_of_tweets)
    except:
            continue
lang_results = comm.gather(lang_count)                  # Data gathering by process 0 from other processes
tweet_results = comm.gather(dict_of_tweets)

if my_rank == 0:
    tweet_result = merge_dictionaries(tweet_results)    # Merging results of all processes
    lang_result = merge_dictionaries(lang_results)
    tweet_result = sort_data(tweet_result)              # Sort and extract top 10 hashtags and languages
    lang_result = sort_data(lang_result)
    print("Top 10 HashTag:")
    print_result(tweet_result)                          # Print result
    print("\nTop 10 Languages:")
    print_result(lang_result)
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
