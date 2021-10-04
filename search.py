# -*- coding: utf-8 -*-

import re

final_output={}

stop_file=open('stopwords.txt','r')
stopwords=stop_file.read().split('\n')

def process_text_file(filename):
    with open(filename, 'r') as myfile:
        text = myfile.read()
    text=text.lower()
    
    return text

def create_word_list(text):
    wordList = re.sub("[^\w]", " ",  text).split()
    
    return wordList

def delete_stop_words(wordList,stopwords):
    for w in wordList:
        if w in stopwords:
            wordList.remove(w)
    return wordList

`
def index(filepath):
    
    for i in range(0,20):
        filename=''
        if(i<9):
            filename='0'+str(i+1)+'.txt.txt'
        else:
            filename=str(i+1)+'.txt.txt'
        text=process_text_file(filepath+filename)
        words=create_word_list(text)
        final_words=delete_stop_words(words,stopwords)##delete the stop words from the original word list
        ##map operation 
        map_output=[]
        for word in final_words:
            map_output.append((word,1))
        ##map operation end
        
        ##reduce operation
        output={}
        for k,v in map_output:
            if k not in output:
                output[k]=0
            output[k]=output[k]+v
        for k,v in output.items():
            if k not in final_output:
                final_output[k]=[]
            final_output[k]+=[(filename[:2],v)]
            
#    fout = "assignment1_dictionary.txt"
#    fo = open(fout, "w")
#
#    for k, v in final_output.items():
#        fo.write(str(k) + ' >>> '+ str(v) + '\n\n')
#
#    fo.close() 
#    
##the index function with the lambda function
def index_lambda(filepath):
    output={}
    
    map_function=lambda x:[(i,1) for i in x]
    reduce=lambda k,v:output[k]+v
    
    
    for i in range(0,20):
        filename=''
        if(i<9):
            filename='0'+str(i+1)+'.txt.txt'
        else:
            filename=str(i+1)+'.txt.txt'
        text=process_text_file(filepath+filename)
        words=create_word_list(text)
        final_words=delete_stop_words(words,stopwords)##delete the stop words from the original word list
        
        map_output=map_function(final_words)
        for k,v in map_output:
            if k not in output:
                output[k]=0
            reduce(k,v)
        
        for k,v in output.items():
            if k not in final_output:
                final_output[k]=[]
            final_output[k]+=[(filename[:2],v)]
    
        
def search(query_string):
    query_string=query_string.lower()
    query_list=create_word_list(query_string)
    print(query_list)
    
    for q in query_list:
        if(q in final_output.keys()):
            print(q+":"+str(final_output[q]))
        else:
            print(q+":The word is not in dictionary.")


index('./Assignment1_data/Assignment1 txt files/') 

#index_lambda('./Assignment1_data/Assignment1 txt files/')           
    
search('Study time is tight.')##example for search function
