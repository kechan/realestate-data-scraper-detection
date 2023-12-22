from typing import List, Dict
import hashlib, pickle, gc
import pandas as pd

from pathlib import Path

def align_truncate_latest_timestamp(dfs: List[pd.DataFrame], timestamp_col: str = 'timestamp'):
  # df latest timestamp are not the same, so we need to truncate such that everyone has the same latest time  
  # as the smallest one 
  cutoff_timestamp = min([df[timestamp_col].max() for df in dfs])

  for df in dfs:
    df.drop(index=df.q(f"{timestamp_col} > '{cutoff_timestamp}'").index, inplace=True)
    df.defrag_index(inplace=True)

def construct_main_df(event_df_dict: Dict) -> pd.DataFrame:
  '''
  Construct dataframe consists of all events
  Each row has event_type to denote the type of event, event_value for 1 primary value for that event

  input should look like:
  {
    'user': {'df': user_df, 'primary_attrib': 'HTTP_USER_AGENT'},
    'listing': {'df': listing_df, 'primary_attrib': 'listingId'},
    'ga_event': {'df': ga_event_df, 'primary_attrib': 'name'},
    'pageview': {'df': pageview_df, 'primary_attrib': 'url'},
    'search': {'df': search_df, 'primary_attrib': None},
    'lead': {'df': lead_df, 'primary_attrib': 'lead_source'}
  }

  '''
  main_df = pd.DataFrame(columns=['user_id', 'timestamp', 'event_type', 'event_value'])

  for event_type, event_info in event_df_dict.items():
    df = event_info['df']
    primary_attrib = event_info['primary_attrib']
    
    _main_df = df[['user_id', 'timestamp']].copy()
    _main_df['event_type'] = event_type
    _main_df['event_value'] = f'{event_type}:' + df[primary_attrib].astype('str') if primary_attrib is not None else None
    
    main_df = pd.concat([main_df, _main_df], axis=0, ignore_index=True)

  del _main_df
  del df
  gc.collect()

  return main_df

class UserIDHasher:
  def __init__(self, hash_map_filename: str, truncate: bool = True, keep_len=15):
    self.hash_map_filename = hash_map_filename
    self.truncate = truncate
    self.keep_len = keep_len

    if Path(self.hash_map_filename).exists():
      self._load_hash_map()
    else:
      self.hash_map = {}

  def hash_user_id(self, user_id):
    if self.truncate:
      hashed_user_id = hashlib.sha256(user_id.encode()).hexdigest()[:self.keep_len]
      if hashed_user_id in self.hash_map and self.hash_map[hashed_user_id] != user_id:
        print(f"Warning: Hash collision detected for user_id {user_id} with {self.hash_map[hashed_user_id]}")
        print(f'consider increasing keep_len')
    else:
      hashed_user_id = hashlib.sha256(user_id.encode()).hexdigest()

    self.hash_map[hashed_user_id] = user_id
    return hashed_user_id
  
  def get_original_user_id(self, hashed_user_id):
    return self.hash_map[hashed_user_id]
  
  def get_hash_user_id(self, original_user_id):
    return self.hash_user_id(original_user_id)

  def get_hash_session_id(self, session_id):
    user_id, id = session_id.rsplit('_', 1)
    hashed_user_id = self.hash_user_id(user_id)
    return f"{hashed_user_id}_{id}"
  
  def get_original_session_id(self, hashed_session_id):
    user_id, id = hashed_session_id.rsplit('_', 1)
    original_user_id = self.get_original_user_id(user_id)
    return f"{original_user_id}_{id}"


  def save(self):
    with open(self.hash_map_filename, 'wb') as f:
      pickle.dump(self.hash_map, f)

  def _load_hash_map(self):
    with open(self.hash_map_filename, 'rb') as f:
      self.hash_map = pickle.load(f)
