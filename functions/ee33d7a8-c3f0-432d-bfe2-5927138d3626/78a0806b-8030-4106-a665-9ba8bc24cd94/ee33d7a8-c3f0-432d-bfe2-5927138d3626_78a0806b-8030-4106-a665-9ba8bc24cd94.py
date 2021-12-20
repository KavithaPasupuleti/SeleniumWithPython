#!/usr/bin/env python
# coding: utf-8

# In[5]:


from xpms_helper.executions.execution_variables import ExecutionVariables


# In[15]:


def benchmark_post_processing(config=None, **kwargs):
    
    exc=ExecutionVariables.get_instance(config['context'])
    gt_urn = exc.get_variable("gt_zip")
    
    return {"gt_urn": gt_urn, "ref_id": config['context']['ref_id'], "plot_bbox": True}
    

