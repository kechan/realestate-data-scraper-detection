from typing import List, Tuple
import pandas as pd

import realestate_core.common.class_extensions
from realestate_core.common.class_extensions import *
from realestate_core.common.utils import load_from_pickle, save_to_pickle, join_df


def load_all_df(
    root_dir: Path
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  
  user_df = pd.read_feather(root_dir/'user_df')
  print(f'len(user_df): {len(user_df)}')

  listing_df = pd.read_feather(root_dir/'listing_df')
  print(f'len(listing_df): {len(listing_df)}')

  ga_event_df = pd.read_feather(root_dir/'ga_event_df')
  print(f'len(ga_event_df): {len(ga_event_df)}')

  pageview_df = pd.read_feather(root_dir/'pageview_df')
  print(f'len(pageview_df): {len(pageview_df)}')

  search_df = pd.read_feather(root_dir/'search_df')
  print(f'len(search_df): {len(search_df)}')

  lead_df = pd.read_feather(root_dir/'lead_df')
  print(f'len(lead_df): {len(lead_df)}')

  return user_df, listing_df, ga_event_df, pageview_df, search_df, lead_df

