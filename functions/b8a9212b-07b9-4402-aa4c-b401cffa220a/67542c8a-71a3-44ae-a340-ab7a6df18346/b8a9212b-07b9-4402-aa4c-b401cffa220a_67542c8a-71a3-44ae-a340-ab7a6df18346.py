#!/usr/bin/env python
# coding: utf-8

# In[5]:


from datetime import datetime
import shutil
from uuid import uuid4

from xpms_file_storage.file_handler import LocalResource, XpmsResource
from xpms_helper.executions.execution_variables import ExecutionVariables


# In[15]:


def upload_benchmark_docs(zip_path, config=None):
    solution_id = config['context']['solution_id']
    remote_source = XpmsResource.get(urn=zip_path)

    lc = LocalResource(key="/tmp/{}/benchmark/{}/{}".format(solution_id, str(uuid4()), remote_source.filename))
    unpack_resource = LocalResource(urn=lc.parent_urn + "/parent_folder")
    remote_source.copy(lc)

    shutil.unpack_archive(lc.fullpath, unpack_resource.fullpath)

    upload_files = [file for file in unpack_resource.list() if file.extension in ".pdf,.jpg,.png"]
    remote_folder_resource = XpmsResource.get(key="{}/benchmark/uploaded_files/{}".format(solution_id, datetime.utcnow().isoformat()))
    all_file_paths = []
    for file in upload_files:
        remote_upload = XpmsResource.get(urn=remote_folder_resource.urn + "/" + file.filename)
        file.copy(remote_upload)
        all_file_paths.append(remote_upload.urn)
    exc=ExecutionVariables.get_instance(config['context'])
    exc.set_variable("gt_zip", zip_path)
    
    return {"file_path": all_file_paths}
    

