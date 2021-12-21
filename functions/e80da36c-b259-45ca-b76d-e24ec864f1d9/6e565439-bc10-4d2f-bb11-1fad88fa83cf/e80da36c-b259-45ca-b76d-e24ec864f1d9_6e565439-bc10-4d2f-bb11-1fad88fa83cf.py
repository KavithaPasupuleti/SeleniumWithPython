#!/usr/bin/env python
# coding: utf-8

# In[5]:


from datetime import datetime


# In[15]:


def transform_process_feedback(config=None, request_type=None, objects=[], **kwargs):
    data = dict(request_type=request_type, objects=objects)
    for i in kwargs:
        if 'root_id' in kwargs[i][0]:
            root_id = kwargs[i][0]['root_id']
        else:
            root_id = kwargs[i][0]['document'][0]['root_id']
        data.update({'root_id': root_id, 'doc_id': root_id})
    return data

