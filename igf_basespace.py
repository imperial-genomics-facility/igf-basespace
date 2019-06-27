import sys,os,re
from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Base,Project,Sample,Experiment,Collection,Collection_group,File,Pipeline,Pipeline_seed,Seqrun,Run,Run_attribute


def fetch_data_and_process_for_basespace_upload(dbconf_file,project_name,flowcell_id):
  '''
  A function for fetching fastq data from db and upload to basespace

  :param dbconf_file: A dbconfig file
  :param project_name: A project name string
  :param flowcell_id: A flowcell_id string
  :returns: A list of sample info with fastq files for basespace upload
  '''
  try:
    dbparam = read_dbconf_json(dbconf_file)
    base = BaseAdaptor(**dbparam)
    base.start_session()
    query = \
    base.\
      session.\
      query(\
        Project.project_igf_id,
        Sample.sample_igf_id,
        Experiment.experiment_igf_id,
        Run.run_igf_id,
        Run_attribute.attribute_name,
        Run_attribute.attribute_value,
        Seqrun.flowcell_id,
        Collection.name,
        Collection.type,
        File.file_path).\
      join(Sample,Project.project_id==Sample.project_id).\
      join(Experiment,Sample.sample_id==Experiment.sample_id).\
      join(Run,Experiment.experiment_id==Run.experiment_id).\
      join(Run_attribute,Run.run_id==Run_attribute.run_id).\
      join(Seqrun,Seqrun.seqrun_id==Run.seqrun_id).\
      join(Collection,Collection.name==Run.run_igf_id).\
      join(Collection_group,Collection.collection_id==Collection_group.collection_id).\
      join(File,File.file_id==Collection_group.file_id).\
      filter(Run_attribute.attribute_name=='R1_READ_COUNT').\
      filter(Collection.type=='demultiplexed_fastq').\
      filter(Collection.table=='run').\
      filter(Seqrun.flowcell_id==flowcell_id).\
      filter(Project.project_igf_id==project_name)
    records = \
      base.fetch_records(\
        query=query,
        output_mode='dataframe')
    base.close_session()
    if len(records.index) == 0:
      raise ValueError('No Fastq records found in db for {0} and {1}'.\
                       format(project_name,flowcell_id))

    sample_info = list()
    for sample,grp in records.groupby('sample_igf_id'):
      sample_entry = dict()
      sample_entry = \
        {'sample_name':str(sample),
         'fastq_path':list(grp['file_path'].values),
         'read_count':str(grp['attribute_value'].values[0]),
         'read_length':'75'}
      sample_info.append(sample_entry)

    if len(sample_info) == 0:
      raise ValueError('No sample info records found for {0} and {1}'.\
                       format(project_name,flowcell_id))

    return sample_info
  except:
    raise

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
      appSession=myAppSession,
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

def __create_sample_and_upload_data(api,appSessionId,appSession,project_id,sample_data_list,exp_name):
  '''
  An internal function for file upload
  
  :param api: An api instance
  :param appSessionId: An active appSessionId
  :param appSession: An active app session
  :param project_id: An existing project_id
  :param sample_data_list: Sample data information
  :param exp_name: Experiment name
  :returns: None
  '''
  try:
    sample_number=0
    for entry in sample_data_list:
      sample_number += 1
      sample_name=entry['sample_name']
      read_count=entry['read_count']
      files=entry['fastq_path']
      read_length=entry['read_length']

      appSession.\
      setStatus(\
        api,
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
