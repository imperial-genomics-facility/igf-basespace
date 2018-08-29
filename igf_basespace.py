import sys,os
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

def create_new_project_and_upload_fastq(project_name,sample_data_list,experiment_name):
  '''
  A function for uploading afstq files to basespace after creating a new project
  
  :param project_name: A project name
  :param sample_data_list: Sample data list containing following information
  
                           sample_name (str)
                           read_count  (str)
                           read_length (str)
                           fastq_path (list)
                           
  :param experiment_name: Experiment name
  :retruns: None
  '''
  try:
    myAPI = BaseSpaceAPI()
    project = myAPI.createProject(project_name)                                 # create new project
    appResults = project.\
                 createAppResult(\
                    myAPI,
                    "FastqUpload",
                    "uploading project data",
                    appSessionId='')                                            # create new app results for project
    myAppSession = appResults.AppSession                                        # get app session
    __create_sample_and_upload_data(\
      api=myAPI,
      appSessionId=myAppSession.Id,
      project_id=project.Id,
      sample_data_list=sample_data_list,
      exp_name=experiment_name
    )                                                                           # upload fastq
    myAppSession.setStatus(myAPI,'complete',"finished file upload")             # mark app session a complete
  except:
    if myAppSession and \
       len(project.getAppResults(myAPI,statuses=['Running']))>0:
      myAppSession.setStatus(myAPI,'complete',"failed file upload")             # comment for failed jobs
    raise

def __create_sample_and_upload_data(api,appSessionId,project_id,sample_data_list,exp_name):
  '''
  An internal function for file upload
  
  :param api: An api instance
  :param appSessionId: An active appSessionId
  :param project_id: An existing project_id
  :param sample_data_list: Sample data information
  :param exp_name: Experiment name
  :returns: None
  '''
  try:
    sample_number=0
    for entry in data:
      sample_number += 1
      sample_name=entry['sample_name']
      read_count=entry['read_count']
      files=entry['fastq_path']
      read_length=entry['read_length']

      myAppSession.\
      setStatus(\
        myAPI,
        'running',
        'uploading {0} of {1} samples'.format(sample_number,
                                              len(sample_data_list)
                                             )
        )                                                                       # comment on running app session
      sample=api.createSample(\
              Id=project_id,
              name=sample_name,
              experimentName=exp_name,
              sampleNumber=sample_number,
              sampleTitle=sample_name,
              readLengths=[read_length,read_length],
              countRaw=read_count,
              countPF=read_count,
              appSessionId=appSessionId
             )                                                                  # create sample
      for local_path in files:
        file=api.sampleFileUpload(\
                Id=sample.Id,
                localPath=local_path,
                fileName=os.path.basename(local_path),
                directory='/FastqUpload/{0}/'.format(sample_name),
                contentType='fastq/gzipped',
              )                                                                 # upload file to sample
  except:
    raise