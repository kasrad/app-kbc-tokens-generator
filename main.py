
import requests
import csv
import pandas as pd
import datetime
from keboola import docker


cfg = docker.Config('/data/')
API_token = cfg.get_parameters()['#api_token']
status = cfg.get_parameters()['status']

PROJECT_ID = cfg.get_parameters()['project_id']
TOKEN = cfg.get_parameters()['#master_token']
EXPIRY = cfg.get_parameters()['expiry_in_seconds']
manage_buckets = bool(cfg.get_parameters()['manage_buckets'])
read_all_f_uploads = bool(cfg.get_parameters()['read_all_f_uploads'])

URL = 'https://connection.keboola.com/manage/projects/{}/tokens'.format(PROJECT_ID)
today = datetime.datetime.today()

def create_tokens(name, expiry):
  """
  function creates tokens
  """
  
  values = {
    "description": str(name),
    "canManageBuckets": manage_buckets,
    "canReadAllFileUploads": read_all_f_uploads,
    "expiresIn": expiry
  }

  headers = {
    "Content-Type": "application/json",
    "X-KBC-ManageApiToken": TOKEN
  }

  r = requests.post(URL, json=values, headers=headers)
  response_body = r.json()
  print(response_body)
  
  return response_body

used_ids = list(pd.read_csv('/data/in/tables/users_sandboxes.csv', usecols = ['ID']).iloc[:,0])
print(list(used_ids))

with open('/data/in/tables/Applicants.csv', mode='rt', encoding='utf-8') as in_file,\
     open('/data/out/tables/tokens.csv', mode='wt', encoding='utf-8') as out_file:
  users = []
  lazy_lines = (line for line in in_file)
  reader = csv.DictReader(lazy_lines, lineterminator='\n')
  
  format_str = '%Y-%m-%d'
  for person in reader:
        if person["receive_sandbox_on"] != '':
            datetime_obj = datetime.datetime.strptime(person["receive_sandbox_on"], format_str)
            days_to_send = (datetime_obj.date() - today.date()).days
            if person["sandbox_sent_on"] == '' and person["ID"] not in used_ids and \
            person["next_round"] == 'Yes' and days_to_send in [-2,-1,0,1,2]:
                email = str(person["email"])
                info = create_tokens(name = email, expiry = EXPIRY)
                user = {"name": person["name"], "ID": person["ID"], "username": info["description"],
                        "token": info["token"],"expiry": info["expires"], "receive_sandbox_on" : person["receive_sandbox_on"]}
                users.append(user)

  print(users)
  keys = (['name', 'ID', 'username', 'token', 'expiry', 'receive_sandbox_on'])
  dict_writer = csv.DictWriter(out_file, keys)
  dict_writer.writeheader()
  dict_writer.writerows(users)
  

print("script done.")
