import pandas as pd
from datetime import date, datetime, timedelta

from ..utils.dataframe import format_timedelta, join_df


one_min = (60. * 1e9)   # 1min is that many nanosec

def compute_aux_dt_session_info(main_df: pd.DataFrame) -> pd.DataFrame:
  '''
  Deduce "sitdown session" from main_df. 
  if the same user_id has been idle for >30min (e.g. lunch break), the last timestamp marks the end of a "sitdown session", 
  and when the user comes back, another distinct "sitdown session" will start. This is to differentiate from the concept of 
  just the "session", which is a persistent cookie on the device that can persist for months. 
  '''

  main_aux_df = main_df.copy() 

  # main_aux_df['datetime'] = main_aux_df.index  # in order to do time diff and other computation 
  # main_aux_df.sort_values(by=['user_id', 'timestamp'], inplace=True)   # already done in main_df

  # For NaN, interprete it as a long time (~1e6 hrs(
  # This is sensible since anything longer than 30min we will regard as the boundary for a sitdown session.

  # get a diff column for datetime
  main_aux_df['t_diff'] = main_aux_df.timestamp.diff().fillna(timedelta(hours=1e6))

  # An indicator variable to denote a switch of user 
  # 1 means user stay the same, 0 means user has switched from the previous row
  main_aux_df['user_diff'] = (main_aux_df.user_id == main_aux_df.user_id.shift()).astype('int')  

  # An indicator variable to denote if t_diff is less than 30min.
  main_aux_df['t_diff_<_30min'] = (main_aux_df.t_diff <= timedelta(minutes=30)).astype('int')


  main_aux_df['delta_t'] = (main_aux_df.user_diff * main_aux_df['t_diff_<_30min']) * main_aux_df.t_diff

  # make a new column 'sitdown_session_id'
  # Note: main_aux_df.user_diff * main_aux_df['t_diff_<_30min'] == 0 marks the boundary of each sitdown session with 1, and "inside" part with 0.

  main_aux_df['sitdown_session_boundary'] = (main_aux_df.user_diff * main_aux_df['t_diff_<_30min'] == 0).astype('int')

  main_aux_df['sitdown_session_id'] = main_aux_df.groupby('user_id').sitdown_session_boundary.cumsum()

  # sitdown_session_id will be of format user_id + '_' + sitdown_session_id
  main_aux_df['sitdown_session_id'] = main_aux_df.user_id.astype('str') + '_' + main_aux_df.sitdown_session_id.astype('str')

  return main_aux_df

def create_sessions_df(main_df: pd.DataFrame) -> pd.DataFrame:
  '''
  Create a dataframe .groupby('sitdown_session_id') with various timings and counts
  as columns after reduced operations
  '''
  assert 'sitdown_session_id' in main_df.columns, 'main_df must have sitdown_session_id column'
  assert 'timestamp' in main_df.columns, 'main_df must have timestamp column'
  assert 'delta_t' in main_df.columns, 'main_df must have delta_t column'
  assert 'event_type' in main_df.columns, 'main_df must have event_type column'
  assert 'event_value' in main_df.columns, 'main_df must have event_value column'

  # duration
  print('Computing session durations...')
  session_durations = main_df.groupby('sitdown_session_id')['timestamp'].agg(['min', 'max', 'count']).rename(columns={'count': 'n_events'})
  session_durations['duration'] = session_durations['max'] - session_durations['min']
  session_durations.reset_index(inplace=True)

  #drop the min and max columns
  session_durations.drop(columns=['min', 'max'], inplace=True)

  session_durations['duration_in_hours'] = session_durations['duration'].dt.total_seconds() / 3600  # convenient for visualization
  session_durations['duration_repr'] = session_durations.duration.apply(format_timedelta)   # this runs long 'cos of .apply TODO: try optimize

  # event type counts (distinct)
  event_type_count = main_df.drop_duplicates(subset=['sitdown_session_id', 'event_type', 'event_value']).groupby('sitdown_session_id')['event_type'].value_counts().unstack(fill_value=0).add_prefix('n_')

  # delta_t
  delta_t = main_df.groupby(['sitdown_session_id']).delta_t.agg(['median'])
  delta_t.rename(columns={'median': 'delta_t_median'}, inplace=True)
  delta_t['delta_t_median_repr'] = delta_t.delta_t_median.apply(format_timedelta)

  # join
  sessions_df = join_df(session_durations, event_type_count, left_on='sitdown_session_id', how='inner')
  sessions_df = join_df(sessions_df, delta_t, left_on='sitdown_session_id', how='inner')

  assert len(sessions_df) == len(session_durations) == len(event_type_count) == len(delta_t), 'join_df should not change the number of rows'

  return sessions_df


def compute_delta_T(main_aux_df):
  '''
  $\delta T$ = 'duration of a sitdown session'
  '''

  dT_stat = pd.DataFrame(main_aux_df.groupby(['user_id', 'sitdown_session_id']).delta_t.sum())     
  dT_stat['delta_t_values'] = dT_stat.delta_t.values.astype('float32')/one_min
  dT_stat = dT_stat.groupby('user_id').delta_t_values.agg(['mean', 'median', 'max', 'min'])       

  dT_stat.columns = ['dT_mean', 'dT_median', 'dT_max', 'dT_min']

  return dT_stat


def compute_Delta_T(main_aux_df):
  '''
  $\Delta T$ = 'time inbetween 2 separate sitdown sessions'
  '''

  qc = np.all([main_aux_df.user_diff != 0, main_aux_df['t_diff_<_30min'] == 0], axis=0)

  DT_stat = main_aux_df[qc].copy() 
  DT_stat['Delta_t_values'] = DT_stat.t_diff.values.astype('float32')/one_min
  DT_stat = DT_stat.groupby('user_id').Delta_t_values.agg(['mean', 'median', 'max', 'min'])
  DT_stat.columns = ['DT_mean', 'DT_median', 'DT_max', 'DT_min']

  return DT_stat


def compute_delta_t(main_aux_df):
  '''
  $\delta t$ = 'time between 2 events within a sitdown session'
  '''

  dt_stat = main_aux_df[['user_id', 'delta_t']].copy()
  dt_stat['delta_t_values'] = dt_stat.delta_t.values.astype('float32')/one_min
  dt_stat = dt_stat.groupby('user_id').delta_t_values.agg(['mean', 'median', 'max', 'min'])
  dt_stat.columns = ['dt_mean', 'dt_median', 'dt_max', 'dt_min']  

  return dt_stat