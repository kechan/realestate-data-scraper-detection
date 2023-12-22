def format_timedelta(td):
  total_seconds = td.total_seconds()
  if total_seconds < 60:
    return f"{total_seconds:.6f}s"
  elif total_seconds < 3600:  # less than an hour
    minutes, seconds = divmod(total_seconds, 60)
    return f"{int(minutes)}m {seconds:.6f}s"
  elif total_seconds < 86400:  # less than a day
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
  else:
    return str(td)
  
def join_df(left, right, left_on, right_on=None, suffix='_y', how='left'):
  if right_on is None: right_on = left_on
  return left.merge(right, how=how, left_on=left_on, right_on=right_on, 
                    suffixes=("", suffix))