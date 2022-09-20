#!/usr/bin/env python
# coding: utf-8

# In[2]:


def countLines(fp):
    for count, line in enumerate(fp):
        pass
    return (count + 1)


# In[2]:


import os
def countWords(fp):
    num_words = 0
    for line in fp:
        line = line.strip(os.linesep)
        wordlist = line.split()
        num_words = num_words + len(wordlist)
    return num_words


# In[4]:


import os
def countChars(fp):
    num_char = 0
    for line in fp:
        line = line.strip(os.linesep)
        wordlist = line.split()
        num_char = num_char + sum(1 for c in line if c not in (os.linesep, ' '))
    return num_char

