#!/usr/bin/env python
# coding: utf-8

# In[5]:


from datetime import datetime


# In[15]:


def aggregate_docs(config=None, **kwargs):
    document = list()
    for k, v in kwargs.items():
        document.append(v[0].get("document", {})[0])
    return dict(document=document)

