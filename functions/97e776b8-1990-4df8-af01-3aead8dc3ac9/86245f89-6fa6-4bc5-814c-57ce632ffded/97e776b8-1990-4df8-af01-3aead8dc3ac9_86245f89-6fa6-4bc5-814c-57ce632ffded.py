#!/usr/bin/env python
# coding: utf-8

# In[5]:


from datetime import datetime


# In[15]:


def retrieve_docs(config=None, **kwargs):
    documents = kwargs.get("document", [])
    if len(documents) > 1:
        del documents[0]
    return dict(document=documents)

